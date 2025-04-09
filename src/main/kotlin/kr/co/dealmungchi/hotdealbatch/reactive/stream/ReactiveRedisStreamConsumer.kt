package kr.co.dealmungchi.hotdealbatch.reactive.stream

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.stream.*
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ReactiveStreamOperations
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Reactive Redis Stream Consumer.
 * Consumes messages from Redis streams using reactive programming patterns.
 * Handles message processing, acknowledgment, and claiming of pending messages.
 */
@Component
class ReactiveRedisStreamConsumer(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val config: RedisStreamConfig,
    private val messageHandler: StreamMessageHandler,
    private val meterRegistry: MeterRegistry
) {
    private val log = LoggerFactory.getLogger(ReactiveRedisStreamConsumer::class.java)

    private val streamSubscriptions = ConcurrentHashMap<String, Disposable>()
    private val running = AtomicBoolean(true)
    private val pendingMessageIds = ConcurrentHashMap<String, MutableMap<String, Instant>>()

    private lateinit var processingTimer: Timer
    private lateinit var messagesProcessedCounter: Counter
    private lateinit var messagesFailedCounter: Counter

    private lateinit var consumerId: String
    private var claimTaskSubscription: Disposable? = null

    /**
     * Initializes the consumer on application startup.
     * Creates consumer groups, starts stream consumption, and schedules claiming of pending messages.
     */
    @EventListener(ApplicationReadyEvent::class)
    fun init() {
        initMetrics()
        consumerId = config.consumer

        setupConsumerGroups()
        startStreamConsumptions()
        scheduleClaimingOfPendingMessages()

        log.info("Reactive Redis Stream Consumer initialized with consumerId: {}", consumerId)
    }

    /**
     * Initializes metrics for monitoring message processing.
     */
    private fun initMetrics() {
        processingTimer = Timer.builder("hotdeal.message.processing.time")
            .description("Time taken to process hot deal messages")
            .register(meterRegistry)

        messagesProcessedCounter = Counter.builder("hotdeal.messages.processed")
            .description("Number of hot deal messages processed successfully")
            .register(meterRegistry)

        messagesFailedCounter = Counter.builder("hotdeal.messages.failed")
            .description("Number of hot deal messages that failed processing")
            .register(meterRegistry)
    }

    /**
     * Sets up consumer groups for all configured stream keys.
     */
    private fun setupConsumerGroups() {
        config.streamKeys.forEach { streamKey ->
            createConsumerGroup(streamKey).subscribe(
                { success -> log.info("Consumer group {} creation for stream {}: {}", config.consumerGroup,
                        streamKey, if (success) "success" else "already exists") },
                { error -> log.error("Failed to create consumer group for stream {}", streamKey, error) }
            )
        }
    }

    /**
     * Starts consumption for all configured stream keys.
     */
    private fun startStreamConsumptions() {
        config.streamKeys.forEach { streamKey ->
            val subscription = startStreamConsumption(streamKey)
            streamSubscriptions[streamKey] = subscription
        }
    }

    /**
     * Schedules periodic claiming of pending messages to ensure no messages are lost.
     */
    private fun scheduleClaimingOfPendingMessages() {
        claimTaskSubscription = Flux.interval(Duration.ofSeconds(30))
            .flatMap { claimPendingMessages() }
            .subscribe()
    }

    /**
     * Creates a consumer group for a given stream key.
     * If the stream doesn't exist, creates it with a dummy message first.
     *
     * @param streamKey The stream key to create a consumer group for
     * @return A Mono that resolves to true if the group was created, false if it already exists
     */
    private fun createConsumerGroup(streamKey: String): Mono<Boolean> {
        val streamOps = reactiveRedisTemplate.opsForStream<Any, Any>()

        return streamOps.size(streamKey)
            .defaultIfEmpty(0L)
            .flatMap { size -> ensureStreamExists(streamOps, streamKey, size) }
            .flatMap { ensureConsumerGroupExists(streamOps, streamKey) }
            .onErrorResume { e ->
                log.error("Error creating consumer group for stream {}: {}", streamKey, e.message)
                Mono.just(false)
            }
    }
    
    /**
     * Ensures that the stream exists by creating it with a dummy message if it doesn't.
     *
     * @param streamOps The stream operations
     * @param streamKey The stream key
     * @param size The current size of the stream
     * @return A Mono that resolves to true when the stream exists
     */
    private fun ensureStreamExists(
        streamOps: ReactiveStreamOperations<String, Any, Any>,
        streamKey: String,
        size: Long
    ): Mono<Boolean> {
        if (size == 0L) {
            val dummy = mapOf<Any, Any>("init" to "init")
            return streamOps.add(StreamRecords.newRecord().`in`(streamKey).ofMap(dummy))
                .map { true }
                .onErrorResume { e ->
                    log.warn("Failed to create stream {}: {}", streamKey, e.message)
                    Mono.just(false)
                }
        }
        return Mono.just(true)
    }
    
    /**
     * Checks if the consumer group exists and creates it if it doesn't.
     *
     * @param streamOps The stream operations
     * @param streamKey The stream key
     * @return A Mono that resolves to true if the group was created, false if it already exists
     */
    private fun ensureConsumerGroupExists(
        streamOps: ReactiveStreamOperations<String, Any, Any>,
        streamKey: String
    ): Mono<Boolean> {
        return streamOps.groups(streamKey).collectList()
            .flatMap { groups ->
                val exists = groups.any { group -> config.consumerGroup == group.groupName() }
                if (exists) Mono.just(false) else createGroupWithFallback(streamOps, streamKey)
            }
    }
    
    /**
     * Creates a consumer group with fallback to read from beginning if $ offset fails.
     *
     * @param streamOps The stream operations
     * @param streamKey The stream key
     * @return A Mono that resolves to true when the group is created
     */
    private fun createGroupWithFallback(
        streamOps: ReactiveStreamOperations<String, Any, Any>,
        streamKey: String
    ): Mono<Boolean> {
        return streamOps.createGroup(streamKey, ReadOffset.from("$"), config.consumerGroup)
            .thenReturn(true)
            .onErrorResume { e ->
                log.warn("Failed to create group from $ offset, trying from 0: {}", e.message)
                streamOps.createGroup(streamKey, ReadOffset.from("0"), config.consumerGroup)
                    .thenReturn(true)
                    .onErrorResume { e2 ->
                        log.error("Failed to create group from 0 offset: {}", e2.message)
                        Mono.just(false)
                    }
            }
    }

    /**
     * Starts consumption of messages from a specific Redis stream.
     *
     * @param streamKey The stream key to consume from
     * @return A Disposable subscription
     */
    private fun startStreamConsumption(streamKey: String): Disposable {
        val streamOps = reactiveRedisTemplate.opsForStream<Any, Any>()
        val consumer = Consumer.from(config.consumerGroup, consumerId)
        
        val readOptions = StreamReadOptions.empty()
            .count(config.batchSize.toLong())
            .block(Duration.ofMillis(config.blockTimeout))

        return Flux.defer { 
                streamOps.read(consumer, readOptions, 
                    StreamOffset.create(streamKey, ReadOffset.lastConsumed()))
            }
            .repeat()
            .flatMap { record -> processStreamRecord(streamKey, record) }
            .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                   .maxBackoff(Duration.ofSeconds(20)))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe()
    }
    
    /**
     * Processes a stream record, tracking it in pending messages and handling it.
     *
     * @param streamKey The stream key
     * @param record The record to process
     * @return A Mono that resolves when processing is complete
     */
    @Suppress("UNCHECKED_CAST")
    private fun processStreamRecord(
        streamKey: String, 
        record: MapRecord<String, Any, Any>
    ): Mono<Boolean> {
        val messageId = record.id.value
        pendingMessageIds.computeIfAbsent(streamKey) { ConcurrentHashMap() }[messageId] = Instant.now()
                
        return processRedisRecord(streamKey, record)
            .doOnSuccess { success ->
                if (success) {
                    pendingMessageIds[streamKey]?.remove(messageId)
                }
            }
    }

    /**
     * Processes a Redis record, converting it to a message and handling it.
     *
     * @param streamKey The stream key
     * @param record The record to process
     * @return A Mono that resolves to true if processing succeeded
     */
    private fun processRedisRecord(
        streamKey: String, 
        record: MapRecord<String, Any, Any>
    ): Mono<Boolean> {
        val messageId = record.id.value
        val entries = record.value

        if (entries.isEmpty()) {
            return acknowledgeMessage(streamKey, messageId).thenReturn(true)
        }

        val message = createRedisStreamMessage(streamKey, messageId, entries)
        val sample = Timer.start()

        return messageHandler.handleMessageReactive(message)
            .timeout(Duration.ofSeconds(10))
            .then(Mono.just(true))
            .flatMap { success -> acknowledgeMessage(streamKey, messageId).thenReturn(true) }
            .doOnSuccess { messagesProcessedCounter.increment() }
            .doOnError { e ->
                messagesFailedCounter.increment()
                log.error("Failed to process message {} from stream {}", messageId, streamKey, e)
            }
            .doFinally { sample.stop(processingTimer) }
            .onErrorResume { e -> handleProcessingError(e, messageId) }
    }
    
    /**
     * Creates a RedisStreamMessage from a record's entries.
     *
     * @param streamKey The stream key
     * @param messageId The message ID
     * @param entries The record entries
     * @return A RedisStreamMessage
     */
    private fun createRedisStreamMessage(
        streamKey: String, 
        messageId: String, 
        entries: Map<Any, Any>
    ): RedisStreamMessage {
        val entry = entries.entries.first()
        val provider = entry.key.toString()
        val data = entry.value.toString()

        return RedisStreamMessage(
            messageId = messageId,
            streamKey = streamKey,
            provider = provider,
            data = data
        )
    }
    
    /**
     * Handles errors during message processing.
     *
     * @param e The exception
     * @param messageId The message ID
     * @return A Mono that resolves to false to indicate processing failure
     */
    private fun handleProcessingError(e: Throwable, messageId: String): Mono<Boolean> {
        if (e is TimeoutException) {
            log.warn("Timeout during processing of message {}", messageId)
        }
        return Mono.just(false)
    }

    /**
     * Acknowledges a message as processed in Redis.
     *
     * @param streamKey The stream key
     * @param messageId The message ID
     * @return A Mono that resolves to the number of messages acknowledged
     */
    private fun acknowledgeMessage(streamKey: String, messageId: String): Mono<Long> {
        val ops = reactiveRedisTemplate.opsForStream<Any, Any>()

        return ops.acknowledge(config.consumerGroup,
                StreamRecords.newRecord().`in`(streamKey).withId(RecordId.of(messageId))
                    .ofMap(emptyMap<Any, Any>()))
            .doOnSuccess { 
                pendingMessageIds[streamKey]?.remove(messageId)
            }
            .onErrorResume { e ->
                log.warn("Failed to acknowledge message {} in stream {}: {}", 
                        messageId, streamKey, e.message)
                Mono.just(0L)
            }
    }

    /**
     * Claims pending messages from all configured stream keys.
     *
     * @return A Mono that completes when all claiming operations are done
     */
    private fun claimPendingMessages(): Mono<Void> {
        if (!running.get()) {
            return Mono.empty()
        }

        return Flux.fromIterable(config.streamKeys)
            .flatMap { claimPendingMessagesFromStream(it) }
            .then()
    }

    /**
     * Claims pending messages from a specific stream.
     *
     * @param streamKey The stream key
     * @return A Mono that completes when claiming is done
     */
    private fun claimPendingMessagesFromStream(streamKey: String): Mono<Void> {
        val ops = reactiveRedisTemplate.opsForStream<Any, Any>()
        val idle = Duration.ofMillis(config.messageClaimMinIdleTime)

        return getPendingMessageIds(ops, streamKey)
            .flatMap { ids ->
                if (ids.isEmpty()) {
                    Mono.empty()
                } else {
                    claimAndProcessMessages(ops, streamKey, idle, ids)
                }
            }
    }
    
    /**
     * Gets IDs of pending messages from a stream.
     *
     * @param ops The stream operations
     * @param streamKey The stream key
     * @return A Mono with a list of pending message IDs
     */
    private fun getPendingMessageIds(
        ops: ReactiveStreamOperations<String, Any, Any>,
        streamKey: String
    ): Mono<List<String>> {
        return ops.pending(streamKey, config.consumerGroup, Range.unbounded<Any>(), 100)
            .flatMapMany { pending ->
                if (pending == null || pending.isEmpty()) {
                    Flux.empty()
                } else {
                    Flux.fromIterable(pending).map { it.idAsString }
                }
            }
            .collectList()
    }
    
    /**
     * Claims and processes pending messages.
     *
     * @param ops The stream operations
     * @param streamKey The stream key
     * @param idle The idle duration threshold for claiming
     * @param ids The IDs of messages to claim
     * @return A Mono that completes when claiming and processing is done
     */
    private fun claimAndProcessMessages(
        ops: ReactiveStreamOperations<String, Any, Any>,
        streamKey: String,
        idle: Duration,
        ids: List<String>
    ): Mono<Void> {
        val recordIds = ids.map { RecordId.of(it) }.toTypedArray()

        return ops.claim(streamKey, config.consumerGroup, consumerId, idle, *recordIds)
            .flatMapIterable { it }
            .flatMap { record ->
                if (record is MapRecord<*, *, *>) {
                    @Suppress("UNCHECKED_CAST")
                    val casted = record as MapRecord<String, Any, Any>
                    val messageId = casted.id.value
                    
                    // Track as pending
                    pendingMessageIds.computeIfAbsent(streamKey) { ConcurrentHashMap() }[messageId] = Instant.now()
                            
                    // Process the record
                    processRedisRecord(streamKey, casted)
                } else {
                    Mono.just(true)
                }
            }
            .then()
    }

    /**
     * Shuts down the consumer, cancelling all subscriptions.
     */
    @PreDestroy
    fun shutdown() {
        running.set(false)
        
        claimTaskSubscription?.takeIf { !it.isDisposed }?.dispose()
        
        streamSubscriptions.values.forEach { subscription ->
            if (!subscription.isDisposed) {
                subscription.dispose()
            }
        }
        
        streamSubscriptions.clear()
        log.info("Reactive Redis Stream Consumer shutdown complete")
    }
}
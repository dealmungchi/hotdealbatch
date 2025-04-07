package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reactive Redis Stream Consumer.
 * Consumes messages from Redis streams using reactive programming patterns.
 * Handles message processing, acknowledgment, and claiming of pending messages.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveRedisStreamConsumer {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final RedisStreamConfig config;
    private final StreamMessageHandler messageHandler;
    private final MeterRegistry meterRegistry;

    private final Map<String, Disposable> streamSubscriptions = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<String, Map<String, Instant>> pendingMessageIds = new ConcurrentHashMap<>();

    private Timer processingTimer;
    private Counter messagesProcessedCounter;
    private Counter messagesFailedCounter;

    private String consumerId;
    private Disposable claimTaskSubscription;

    /**
     * Initializes the consumer on application startup.
     * Creates consumer groups, starts stream consumption, and schedules claiming of pending messages.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        initMetrics();
        consumerId = config.getConsumer();

        setupConsumerGroups();
        startStreamConsumptions();
        scheduleClaimingOfPendingMessages();

        log.info("Reactive Redis Stream Consumer initialized with consumerId: {}", consumerId);
    }

    /**
     * Initializes metrics for monitoring message processing.
     */
    private void initMetrics() {
        processingTimer = Timer.builder("hotdeal.message.processing.time")
                .description("Time taken to process hot deal messages")
                .register(meterRegistry);

        messagesProcessedCounter = Counter.builder("hotdeal.messages.processed")
                .description("Number of hot deal messages processed successfully")
                .register(meterRegistry);

        messagesFailedCounter = Counter.builder("hotdeal.messages.failed")
                .description("Number of hot deal messages that failed processing")
                .register(meterRegistry);
    }

    /**
     * Sets up consumer groups for all configured stream keys.
     */
    private void setupConsumerGroups() {
        config.getStreamKeys().forEach(streamKey -> {
            createConsumerGroup(streamKey).subscribe(
                    success -> log.info("Consumer group {} creation for stream {}: {}", config.getConsumerGroup(),
                            streamKey, success ? "success" : "already exists"),
                    error -> log.error("Failed to create consumer group for stream {}", streamKey, error));
        });
    }

    /**
     * Starts consumption for all configured stream keys.
     */
    private void startStreamConsumptions() {
        config.getStreamKeys().forEach(streamKey -> {
            Disposable subscription = startStreamConsumption(streamKey);
            streamSubscriptions.put(streamKey, subscription);
        });
    }

    /**
     * Schedules periodic claiming of pending messages to ensure no messages are lost.
     */
    private void scheduleClaimingOfPendingMessages() {
        claimTaskSubscription = Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> claimPendingMessages())
                .subscribe();
    }

    /**
     * Creates a consumer group for a given stream key.
     * If the stream doesn't exist, creates it with a dummy message first.
     *
     * @param streamKey The stream key to create a consumer group for
     * @return A Mono that resolves to true if the group was created, false if it already exists
     */
    private Mono<Boolean> createConsumerGroup(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();

        return streamOps.size(streamKey)
                .defaultIfEmpty(0L)
                .flatMap(size -> ensureStreamExists(streamOps, streamKey, size))
                .flatMap(ready -> checkAndCreateGroup(streamOps, streamKey))
                .onErrorResume(e -> {
                    log.error("Error creating consumer group for stream {}: {}", streamKey, e.getMessage());
                    return Mono.just(false);
                });
    }
    
    /**
     * Ensures that the stream exists by creating it with a dummy message if it doesn't.
     *
     * @param streamOps The stream operations
     * @param streamKey The stream key
     * @param size The current size of the stream
     * @return A Mono that resolves to true when the stream exists
     */
    private Mono<Boolean> ensureStreamExists(ReactiveStreamOperations<String, Object, Object> streamOps, 
                                            String streamKey, Long size) {
        if (size == 0) {
            Map<String, String> dummy = Collections.singletonMap("init", "init");
            return streamOps.add(StreamRecords.newRecord().in(streamKey).ofMap(dummy))
                    .map(r -> true)
                    .onErrorResume(e -> {
                        log.warn("Failed to create stream {}: {}", streamKey, e.getMessage());
                        return Mono.just(false);
                    });
        }
        return Mono.just(true);
    }
    
    /**
     * Checks if the consumer group exists and creates it if it doesn't.
     *
     * @param streamOps The stream operations
     * @param streamKey The stream key
     * @return A Mono that resolves to true if the group was created, false if it already exists
     */
    private Mono<Boolean> checkAndCreateGroup(ReactiveStreamOperations<String, Object, Object> streamOps, String streamKey) {
        return streamOps.groups(streamKey).collectList()
                .flatMap(groups -> {
                    boolean exists = groups.stream()
                            .anyMatch(g -> config.getConsumerGroup().equals(g.groupName()));
                    if (exists)
                        return Mono.just(false);
                    
                    return createGroupWithFallback(streamOps, streamKey);
                });
    }
    
    /**
     * Creates a consumer group with fallback to read from beginning if $ offset fails.
     *
     * @param streamOps The stream operations
     * @param streamKey The stream key
     * @return A Mono that resolves to true when the group is created
     */
    private Mono<Boolean> createGroupWithFallback(ReactiveStreamOperations<String, Object, Object> streamOps, String streamKey) {
        return streamOps.createGroup(streamKey, ReadOffset.from("$"), config.getConsumerGroup())
                .thenReturn(true)
                .onErrorResume(e -> {
                    log.warn("Failed to create group from $ offset, trying from 0: {}", e.getMessage());
                    return streamOps.createGroup(streamKey, ReadOffset.from("0"), config.getConsumerGroup())
                            .thenReturn(true)
                            .onErrorResume(e2 -> {
                                log.error("Failed to create group from 0 offset: {}", e2.getMessage());
                                return Mono.just(false);
                            });
                });
    }

    /**
     * Starts consumption of messages from a specific Redis stream.
     *
     * @param streamKey The stream key to consume from
     * @return A Disposable subscription
     */
    @SuppressWarnings("unchecked")
    private Disposable startStreamConsumption(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();
        Consumer consumer = Consumer.from(config.getConsumerGroup(), consumerId);
        
        StreamReadOptions readOptions = StreamReadOptions.empty()
                .count(config.getBatchSize())
                .block(Duration.ofMillis(config.getBlockTimeout()));

        return Flux.defer(() -> streamOps.read(consumer, readOptions, 
                    StreamOffset.create(streamKey, ReadOffset.lastConsumed())))
                .repeat()
                .flatMap(record -> processStreamRecord(streamKey, record))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                           .maxBackoff(Duration.ofSeconds(20)))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }
    
    /**
     * Processes a stream record, tracking it in pending messages and handling it.
     *
     * @param streamKey The stream key
     * @param record The record to process
     * @return A Mono that resolves when processing is complete
     */
    private Mono<Boolean> processStreamRecord(String streamKey, MapRecord<String, Object, Object> record) {
        String messageId = record.getId().getValue();
        pendingMessageIds.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                .put(messageId, Instant.now());
                
        return processRedisRecord(streamKey, record)
                .doOnSuccess(success -> {
                    if (success) {
                        pendingMessageIds.getOrDefault(streamKey, Collections.emptyMap())
                                .remove(messageId);
                    }
                });
    }

    /**
     * Processes a Redis record, converting it to a message and handling it.
     *
     * @param streamKey The stream key
     * @param record The record to process
     * @return A Mono that resolves to true if processing succeeded
     */
    private Mono<Boolean> processRedisRecord(String streamKey, MapRecord<String, Object, Object> record) {
        String messageId = record.getId().getValue();
        Map<Object, Object> entries = record.getValue();

        if (entries.isEmpty()) {
            return acknowledgeMessage(streamKey, messageId).thenReturn(true);
        }

        RedisStreamMessage message = createMessage(streamKey, messageId, entries);
        Timer.Sample sample = Timer.start();

        return messageHandler.handleMessageReactive(message)
                .timeout(Duration.ofSeconds(10))
                .then(Mono.just(true))
                .flatMap(success -> acknowledgeMessage(streamKey, messageId).thenReturn(true))
                .doOnSuccess(success -> messagesProcessedCounter.increment())
                .doOnError(e -> {
                    messagesFailedCounter.increment();
                    log.error("Failed to process message {} from stream {}", messageId, streamKey, e);
                })
                .doFinally(signal -> sample.stop(processingTimer))
                .onErrorResume(e -> handleProcessingError(e, messageId));
    }
    
    /**
     * Creates a RedisStreamMessage from a record's entries.
     *
     * @param streamKey The stream key
     * @param messageId The message ID
     * @param entries The record entries
     * @return A RedisStreamMessage
     */
    private RedisStreamMessage createMessage(String streamKey, String messageId, Map<Object, Object> entries) {
        Map.Entry<Object, Object> entry = entries.entrySet().iterator().next();
        String provider = entry.getKey().toString();
        String data = entry.getValue().toString();

        return RedisStreamMessage.builder()
                .messageId(messageId)
                .streamKey(streamKey)
                .provider(provider)
                .data(data)
                .build();
    }
    
    /**
     * Handles errors during message processing.
     *
     * @param e The exception
     * @param messageId The message ID
     * @return A Mono that resolves to false to indicate processing failure
     */
    private Mono<Boolean> handleProcessingError(Throwable e, String messageId) {
        if (e instanceof TimeoutException) {
            log.warn("Timeout during processing of message {}", messageId);
        }
        return Mono.just(false);
    }

    /**
     * Acknowledges a message as processed in Redis.
     *
     * @param streamKey The stream key
     * @param messageId The message ID
     * @return A Mono that resolves to the number of messages acknowledged
     */
    private Mono<Long> acknowledgeMessage(String streamKey, String messageId) {
        ReactiveStreamOperations<String, Object, Object> ops = reactiveRedisTemplate.opsForStream();

        return ops.acknowledge(config.getConsumerGroup(),
                StreamRecords.newRecord().in(streamKey).withId(RecordId.of(messageId))
                        .ofMap(Collections.emptyMap()))
                .doOnSuccess(result -> {
                    Map<String, Instant> pending = pendingMessageIds.get(streamKey);
                    if (pending != null)
                        pending.remove(messageId);
                })
                .onErrorResume(e -> {
                    log.warn("Failed to acknowledge message {} in stream {}: {}", 
                            messageId, streamKey, e.getMessage());
                    return Mono.just(0L);
                });
    }

    /**
     * Claims pending messages from all configured stream keys.
     *
     * @return A Mono that completes when all claiming operations are done
     */
    private Mono<Void> claimPendingMessages() {
        if (!running.get()) {
            return Mono.empty();
        }

        return Flux.fromIterable(config.getStreamKeys())
                .flatMap(this::claimPendingMessagesFromStream)
                .then();
    }

    /**
     * Claims pending messages from a specific stream.
     *
     * @param streamKey The stream key
     * @return A Mono that completes when claiming is done
     */
    private Mono<Void> claimPendingMessagesFromStream(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> ops = reactiveRedisTemplate.opsForStream();
        Duration idle = Duration.ofMillis(config.getMessageClaimMinIdleTime());

        return getPendingMessageIds(ops, streamKey)
                .flatMap(ids -> {
                    if (ids.isEmpty()) {
                        return Mono.empty();
                    }
                    return claimAndProcessMessages(ops, streamKey, idle, ids);
                });
    }
    
    /**
     * Gets IDs of pending messages from a stream.
     *
     * @param ops The stream operations
     * @param streamKey The stream key
     * @return A Mono with a list of pending message IDs
     */
    private Mono<List<String>> getPendingMessageIds(ReactiveStreamOperations<String, Object, Object> ops, String streamKey) {
        return ops.pending(streamKey, config.getConsumerGroup(), Range.unbounded(), 100)
                .flatMapMany(pending -> {
                    if (pending == null || pending.isEmpty()) {
                        return Flux.empty();
                    }
                    return Flux.fromIterable(pending).map(PendingMessage::getIdAsString);
                })
                .collectList();
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
    private Mono<Void> claimAndProcessMessages(ReactiveStreamOperations<String, Object, Object> ops, 
                                             String streamKey, Duration idle, List<String> ids) {
        RecordId[] recordIds = ids.stream().map(RecordId::of).toArray(RecordId[]::new);

        return ops.claim(streamKey, config.getConsumerGroup(), consumerId, idle, recordIds)
                .flatMapIterable(claimedList -> claimedList)
                .flatMap(record -> {
                    if (record instanceof MapRecord<?, ?, ?> mapRecord) {
                        @SuppressWarnings("unchecked")
                        MapRecord<String, Object, Object> casted = (MapRecord<String, Object, Object>) mapRecord;
                        String messageId = casted.getId().getValue();
                        
                        // Track as pending
                        pendingMessageIds.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                                .put(messageId, Instant.now());
                                
                        // Process the record
                        return processRedisRecord(streamKey, casted);
                    }
                    return Mono.just(true);
                })
                .then();
    }

    /**
     * Shuts down the consumer, cancelling all subscriptions.
     */
    @PreDestroy
    public void shutdown() {
        running.set(false);
        
        if (claimTaskSubscription != null && !claimTaskSubscription.isDisposed()) {
            claimTaskSubscription.dispose();
        }
        
        streamSubscriptions.values().forEach(subscription -> {
            if (!subscription.isDisposed()) {
                subscription.dispose();
            }
        });
        
        streamSubscriptions.clear();
        log.info("Reactive Redis Stream Consumer shutdown complete");
    }
}

package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private Counter messagesReclaimedCounter;
    
    private String consumerId;
    private Disposable claimTaskSubscription;
    
    @PostConstruct
    public void init() {
        initMetrics();
        consumerId = config.getConsumerPrefix() + UUID.randomUUID();
        
        // Create consumer groups for all partitions if they don't exist
        config.getStreamKeys().forEach(streamKey -> {
            createConsumerGroup(streamKey)
                .subscribe(
                    success -> log.info("Consumer group {} creation for stream {}: {}", 
                                      config.getConsumerGroup(), streamKey, success ? "success" : "already exists"),
                    error -> log.error("Failed to create consumer group for stream {}", streamKey, error)
                );
        });
        
        // Start consuming from all partitions
        config.getStreamKeys().forEach(streamKey -> {
            Disposable subscription = startStreamConsumption(streamKey);
            streamSubscriptions.put(streamKey, subscription);
        });
        
        // Start periodic task to claim pending messages
        claimTaskSubscription = Flux.interval(Duration.ofSeconds(30))
            .flatMap(tick -> claimPendingMessages())
            .subscribe();
        
        log.info("Reactive Redis Stream Consumer initialized with consumerId: {}", consumerId);
    }
    
    private void initMetrics() {
        log.info("Redis Stream Config: streamKeyPrefix={}, partitions={}, consumerGroup={}, consumerPrefix={}",
                config.getStreamKeyPrefix(), config.getPartitions(), config.getConsumerGroup(), config.getConsumerPrefix());
        processingTimer = Timer.builder("hotdeal.message.processing.time")
                .description("Time taken to process hot deal messages")
                .register(meterRegistry);
        
        messagesProcessedCounter = Counter.builder("hotdeal.messages.processed")
                .description("Number of hot deal messages processed successfully")
                .register(meterRegistry);
        
        messagesFailedCounter = Counter.builder("hotdeal.messages.failed")
                .description("Number of hot deal messages that failed processing")
                .register(meterRegistry);
        
        messagesReclaimedCounter = Counter.builder("hotdeal.messages.reclaimed")
                .description("Number of hot deal messages reclaimed from pending state")
                .register(meterRegistry);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Mono<Boolean> createConsumerGroup(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();
        
        // First check if the stream exists, if not create it with a dummy message
        return streamOps.size(streamKey)
            .defaultIfEmpty(0L)
            .flatMap(size -> {
                if (size == 0) {
                    // Stream doesn't exist, add a dummy record to create it
                    Map<String, String> dummyMap = Collections.singletonMap("init", "init");
                    return streamOps.add(StreamRecords.newRecord()
                                .in(streamKey)
                                .ofMap((Map) dummyMap))
                                .map(recordId -> true)
                                .onErrorResume(e -> {
                                    log.warn("Error creating stream {}", streamKey, e);
                                    return Mono.just(false);
                                });
                }
                return Mono.just(true);
            })
            .flatMap(streamCreated -> {
                // Now check if the consumer group exists
                return streamOps.groups(streamKey)
                    .collectList()
                    .flatMap(groups -> {
                        // Check if our consumer group already exists
                        boolean groupExists = groups.stream()
                                .anyMatch(group -> config.getConsumerGroup().equals(group.groupName()));
                                
                        if (groupExists) {
                            log.info("Consumer group '{}' already exists for stream '{}'", 
                                config.getConsumerGroup(), streamKey);
                            return Mono.just(false);
                        }
                        
                        // Group doesn't exist, create it - try with special symbol $ first
                        log.debug("Creating consumer group '{}' for stream '{}'", config.getConsumerGroup(), streamKey);
                        // Try with "$" for most Redis versions
                        return streamOps.createGroup(streamKey, ReadOffset.from("$"), config.getConsumerGroup())
                            .doOnSuccess(success -> log.debug("Successfully created consumer group with $ ID"))
                            .map(success -> true)
                            .onErrorResume(e -> {
                                log.warn("Failed to create consumer group with $ offset: {}", e.getMessage());
                                
                                // Try with 0
                                return streamOps.createGroup(streamKey, ReadOffset.from("0"), config.getConsumerGroup())
                                    .doOnSuccess(success -> log.debug("Successfully created consumer group with 0 ID"))
                                    .map(success -> true)
                                    .onErrorResume(e2 -> {
                                        log.warn("Failed to create consumer group with 0 offset: {}", e2.getMessage());
                                        
                                        // Final attempt with latest()
                                        return streamOps.createGroup(streamKey, ReadOffset.latest(), config.getConsumerGroup())
                                            .doOnSuccess(success -> log.debug("Successfully created consumer group with latest() offset"))
                                            .map(success -> true)
                                            .onErrorResume(e3 -> {
                                                // Still handle BUSYGROUP as a fallback (in case of race condition)
                                                if (e3 instanceof io.lettuce.core.RedisBusyException 
                                                        || (e3.getMessage() != null && e3.getMessage().contains("BUSYGROUP"))) {
                                                    log.info("Consumer group '{}' already exists for stream '{}'", 
                                                            config.getConsumerGroup(), streamKey);
                                                    return Mono.just(false);
                                                }
                                                log.error("All attempts to create consumer group failed. Last error: {}", 
                                                        e3.getMessage(), e3);
                                                return Mono.just(false);  // Return false to prevent error propagation
                                            });
                                    });
                            });
                    })
                    .onErrorResume(e -> {
                        log.error("Error checking/creating consumer group for stream {}: {}", 
                                streamKey, e.getMessage());
                        return Mono.just(false);
                    });
            });
    }
    
    @SuppressWarnings("unchecked")
    private Disposable startStreamConsumption(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();
        Consumer consumer = Consumer.from(config.getConsumerGroup(), consumerId);
        
        return Flux.interval(Duration.ofMillis(config.getPollTimeout()))
            .flatMap(tick -> {
                if (!running.get()) {
                    return Flux.empty();
                }
                
                StreamReadOptions readOptions = StreamReadOptions.empty()
                        .count(config.getBatchSize())
                        .block(Duration.ofMillis(config.getBlockTimeout()));
                
                return streamOps.read(consumer, readOptions, 
                        StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
            })
            .flatMap(record -> {
                // Track message as pending
                String messageId = record.getId().getValue();
                pendingMessageIds.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                   .put(messageId, Instant.now());
                
                return processRedisRecord(streamKey, record)
                    .doOnSuccess(success -> {
                        if (success) {
                            // Remove from pending tracking on success
                            pendingMessageIds.getOrDefault(streamKey, Collections.emptyMap()).remove(messageId);
                        }
                    });
            })
            .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(20))
                .doBeforeRetry(signal -> log.warn("Retrying stream consumption after error: {}", 
                                                signal.failure().getMessage())))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                success -> log.debug("stream subscription completed for {}", streamKey),
                error -> log.error("Unexpected error in stream subscription", error)
            );
    }
    
    private Mono<Boolean> processRedisRecord(String streamKey, MapRecord<String, Object, Object> record) {
        String messageId = record.getId().getValue();
        Map<Object, Object> entries = record.getValue();
        
        if (entries.isEmpty()) {
            // Empty record, just acknowledge
            return acknowledgeMessage(streamKey, messageId)
                .thenReturn(true);
        }
        
        // We expect only one entry per record as per the sample Redis CLI output
        Map.Entry<Object, Object> firstEntry = entries.entrySet().iterator().next();
        String provider = firstEntry.getKey().toString();
        String data = firstEntry.getValue().toString();
        
        RedisStreamMessage message = RedisStreamMessage.builder()
                .messageId(messageId)
                .streamKey(streamKey)
                .provider(provider)
                .data(data)
                .build();
        
        Timer.Sample sample = Timer.start();
        
        // Use the interface method directly
        return messageHandler.handleMessageReactive(message)
            .then(Mono.just(true))
            .doOnSuccess(success -> {
                sample.stop(processingTimer);
                messagesProcessedCounter.increment();
                log.debug("Successfully processed message {} from stream {}", messageId, streamKey);
            })
            .<Boolean>flatMap(success -> acknowledgeMessage(streamKey, messageId).thenReturn(true))
            .doOnError(e -> {
                sample.stop(processingTimer);
                messagesFailedCounter.increment();
                log.error("Failed to process message {} from stream {}: {}", 
                    messageId, streamKey, e.getMessage(), e);
            })
            .onErrorResume(e -> Mono.just(false));
    }
    
    private Mono<Long> acknowledgeMessage(String streamKey, String messageId) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();
        
        log.debug("Acknowledging message {} on stream {} for group {}", 
                messageId, streamKey, config.getConsumerGroup());
        
        // 방법 1: MapRecord 사용
        return Mono.defer(() -> {
            try {
                // 방법 1: MapRecord를 사용하여 확인
                MapRecord<String, Object, Object> record = StreamRecords.newRecord()
                        .in(streamKey)
                        .withId(RecordId.of(messageId))
                        .ofMap(Collections.emptyMap());
                
                return streamOps.acknowledge(config.getConsumerGroup(), record);
            } catch (Exception e) {
                log.warn("Failed to acknowledge with MapRecord: {}", e.getMessage());
                
                // 방법 2: 기존 방식 시도
                return streamOps.acknowledge(config.getConsumerGroup(), 
                        org.springframework.data.redis.connection.stream.Record.of(messageId).withStreamKey(streamKey));
            }
        })
        .doOnSuccess(result -> {
            log.debug("Successfully acknowledged message {} on stream {}, result: {}", 
                    messageId, streamKey, result);
            // Remove from pending tracking
            Map<String, Instant> streamPending = pendingMessageIds.get(streamKey);
            if (streamPending != null) {
                streamPending.remove(messageId);
            }
        })
        .onErrorResume(e -> {
            log.error("Failed to acknowledge message {} on stream {}: {}", 
                    messageId, streamKey, e.getMessage(), e);
            
            // 오류가 발생해도 메시지 처리 체인에 영향을 주지 않도록 0을 반환
            return Mono.just(0L);
        });
    }
    
    private Mono<Void> claimPendingMessages() {
        if (!running.get()) {
            return Mono.empty();
        }
        
        return Flux.fromIterable(config.getStreamKeys())
            .flatMap(streamKey -> {
                return claimPendingMessagesFromStream(streamKey);
            })
            .then();
    }
    
    private Mono<Void> claimPendingMessagesFromStream(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();
        Instant threshold = Instant.now().minusMillis(config.getMessageClaimMinIdleTime());
        
        return streamOps.pending(streamKey, config.getConsumerGroup(), Range.unbounded(), 100)
            .flatMapMany(pendingMessages -> {
                if (pendingMessages == null || pendingMessages.isEmpty()) {
                    return Flux.empty();
                }
                
                return Flux.fromIterable(pendingMessages)
                    .filter(pendingMessage -> {
                        String pendingMessageId = pendingMessage.getIdAsString();
                        String consumerName = pendingMessage.getConsumerName();
                        // Set a default time - assume message was delivered recently
                        Instant lastDelivery = Instant.now().minus(Duration.ofMillis(1000));
                        
                        // Skip our own pending messages that aren't idle for long enough
                        if (consumerName.equals(consumerId) && lastDelivery.isAfter(threshold)) {
                            return false;
                        }
                        
                        // Skip messages we're already processing
                        Map<String, Instant> pending = pendingMessageIds.getOrDefault(streamKey, new HashMap<>());
                        if (pending.containsKey(pendingMessageId) && 
                            pending.get(pendingMessageId).isAfter(threshold)) {
                            return false;
                        }
                        
                        // Only reclaim messages that are idle long enough
                        return lastDelivery.isBefore(threshold);
                    })
                    .map(PendingMessage::getIdAsString)
                    .collectList();
            })
            .flatMap(messageIds -> {
                if (messageIds.isEmpty()) {
                    return Mono.empty();
                }
                
                log.info("Claiming {} pending messages from stream {}", messageIds.size(), streamKey);
                messagesReclaimedCounter.increment(messageIds.size());
                
                // Convert String IDs to RecordId objects
                RecordId[] recordIds = messageIds.stream()
                    .map(id -> RecordId.of(id))
                    .toArray(RecordId[]::new);
                
                return streamOps.claim(streamKey, config.getConsumerGroup(), consumerId, 
                        Duration.ofMillis(config.getMessageClaimMinIdleTime()), recordIds)
                        .flatMap(claimedRecord -> {
                            if (claimedRecord instanceof MapRecord) {
                                @SuppressWarnings("unchecked")
                                MapRecord<String, Object, Object> mapRecord = (MapRecord<String, Object, Object>) claimedRecord;
                                String claimedId = mapRecord.getId().getValue();
                                
                                // Mark as pending under our consumer
                                pendingMessageIds.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                                   .put(claimedId, Instant.now());
                                
                                return processRedisRecord(streamKey, mapRecord).then();
                            }
                            return Mono.empty();
                        });
            })
            .then();
    }
    
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Reactive Redis Stream Consumer");
        running.set(false);
        
        // Cancel claim task
        if (claimTaskSubscription != null && !claimTaskSubscription.isDisposed()) {
            claimTaskSubscription.dispose();
        }
        
        // Cancel all stream subscriptions
        streamSubscriptions.values().forEach(disposable -> {
            if (!disposable.isDisposed()) {
                disposable.dispose();
            }
        });
        
        streamSubscriptions.clear();
    }
}
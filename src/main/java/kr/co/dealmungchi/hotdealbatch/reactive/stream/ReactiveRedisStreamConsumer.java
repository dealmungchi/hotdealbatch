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

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        initMetrics();
        consumerId = config.getConsumer();

        // Create a consumer group for regular streams only
        config.getStreamKeys().forEach(streamKey -> {
            createConsumerGroup(streamKey).subscribe(
                    success -> log.info("Consumer group {} creation for stream {}: {}", config.getConsumerGroup(),
                            streamKey, success ? "success" : "already exists"),
                    error -> log.error("Failed to create consumer group for stream {}", streamKey, error));
        });

        // Start consumption for regular streams only
        config.getStreamKeys().forEach(streamKey -> {
            Disposable subscription = startStreamConsumption(streamKey);
            streamSubscriptions.put(streamKey, subscription);
        });

        // Note: We don't create consumer groups for or consume the new hot deals
        // streams
        // since they will be consumed by other systems

        // Periodic reclaiming of pending messages
        claimTaskSubscription = Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> claimPendingMessages())
                .subscribe();

        log.info("Reactive Redis Stream Consumer initialized with consumerId: {}", consumerId);
    }

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

    private Mono<Boolean> createConsumerGroup(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();

        return streamOps.size(streamKey).defaultIfEmpty(0L)
                .flatMap(size -> {
                    if (size == 0) {
                        Map<String, String> dummy = Collections.singletonMap("init", "init");
                        return streamOps.add(StreamRecords.newRecord().in(streamKey).ofMap(dummy))
                                .map(r -> true).onErrorResume(e -> Mono.just(false));
                    }
                    return Mono.just(true);
                })
                .flatMap(ready -> streamOps.groups(streamKey).collectList()
                        .flatMap(groups -> {
                            boolean exists = groups.stream()
                                    .anyMatch(g -> config.getConsumerGroup().equals(g.groupName()));
                            if (exists)
                                return Mono.just(false);
                            return streamOps.createGroup(streamKey, ReadOffset.from("$"), config.getConsumerGroup())
                                    .thenReturn(true)
                                    .onErrorResume(e -> streamOps
                                            .createGroup(streamKey, ReadOffset.from("0"), config.getConsumerGroup())
                                            .thenReturn(true).onErrorResume(e2 -> Mono.just(false)));
                        }))
                .onErrorResume(e -> Mono.just(false));
    }

    @SuppressWarnings("unchecked")
    private Disposable startStreamConsumption(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> streamOps = reactiveRedisTemplate.opsForStream();
        Consumer consumer = Consumer.from(config.getConsumerGroup(), consumerId);

        return Flux.defer(() -> streamOps.read(consumer,
                StreamReadOptions.empty().count(config.getBatchSize())
                        .block(Duration.ofMillis(config.getBlockTimeout())),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed())))
                .repeat()
                .flatMap(record -> {
                    String messageId = record.getId().getValue();
                    pendingMessageIds.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                            .put(messageId, Instant.now());
                    return processRedisRecord(streamKey, record).doOnSuccess(success -> {
                        if (success)
                            pendingMessageIds.getOrDefault(streamKey, Collections.emptyMap()).remove(messageId);
                    });
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(20)))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    private Mono<Boolean> processRedisRecord(String streamKey, MapRecord<String, Object, Object> record) {
        String messageId = record.getId().getValue();
        Map<Object, Object> entries = record.getValue();

        if (entries.isEmpty())
            return acknowledgeMessage(streamKey, messageId).thenReturn(true);

        Map.Entry<Object, Object> entry = entries.entrySet().iterator().next();
        String provider = entry.getKey().toString();
        String data = entry.getValue().toString();

        RedisStreamMessage message = RedisStreamMessage.builder()
                .messageId(messageId).streamKey(streamKey)
                .provider(provider).data(data).build();

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
                .onErrorResume(e -> {
                    if (e instanceof TimeoutException) {
                        log.warn("Timeout during processing of message {}", messageId);
                    }
                    return Mono.just(false);
                });
    }

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
                .onErrorResume(e -> Mono.just(0L));
    }

    private Mono<Void> claimPendingMessages() {
        if (!running.get())
            return Mono.empty();

        // Only claim pending messages from regular streams (not new hot deals streams)
        return Flux.fromIterable(config.getStreamKeys())
                .flatMap(this::claimPendingMessagesFromStream)
                .then();
    }

    private Mono<Void> claimPendingMessagesFromStream(String streamKey) {
        ReactiveStreamOperations<String, Object, Object> ops = reactiveRedisTemplate.opsForStream();
        Duration idle = Duration.ofMillis(config.getMessageClaimMinIdleTime());

        return ops.pending(streamKey, config.getConsumerGroup(), Range.unbounded(), 100)
                .flatMapMany(pending -> {
                    if (pending == null || pending.isEmpty())
                        return Flux.empty();
                    return Flux.fromIterable(pending).map(PendingMessage::getIdAsString);
                })
                .collectList()
                .flatMap(ids -> {
                    if (ids.isEmpty())
                        return Mono.empty();
                    RecordId[] recordIds = ids.stream().map(RecordId::of).toArray(RecordId[]::new);

                    return ops.claim(streamKey, config.getConsumerGroup(), consumerId, idle, recordIds)
                            .flatMap(claimedList -> Flux.fromIterable(claimedList))
                            .flatMap(record -> {
                                if (record instanceof MapRecord<?, ?, ?> mapRecord) {
                                    @SuppressWarnings("unchecked")
                                    MapRecord<String, Object, Object> casted = (MapRecord<String, Object, Object>) mapRecord;
                                    pendingMessageIds.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                                            .put(casted.getId().getValue(), Instant.now());
                                    return processRedisRecord(streamKey, casted).then();
                                }
                                return Mono.empty();
                            })
                            .then();
                });
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (claimTaskSubscription != null && !claimTaskSubscription.isDisposed()) {
            claimTaskSubscription.dispose();
        }
        streamSubscriptions.values().forEach(s -> {
            if (!s.isDisposed())
                s.dispose();
        });
        streamSubscriptions.clear();
    }
}

package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import kr.co.dealmungchi.hotdealbatch.domain.entity.HotDeal;
import kr.co.dealmungchi.hotdealbatch.domain.repository.HotDealRepository;
import kr.co.dealmungchi.hotdealbatch.domain.service.HotdealService;
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
import kr.co.dealmungchi.hotdealbatch.service.ProviderCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveHotDealProcessor {

    private final HotDealRepository repository;
    private final HotdealService hotdealService;
    private final ProviderCacheService providerCacheService;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final RedisStreamConfig streamConfig;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    // Maximum length for new hot deals streams to prevent unbounded growth
    private static final int NEW_HOTDEALS_STREAM_MAX_LENGTH = 100;
    
    // Sink: Buffers up to 100 items while managing backpressure
    // Using replay instead of multicast to ensure the sink buffers items even without active subscribers
    private final Sinks.Many<HotDealDto> sink = Sinks.many().replay().limit(100);

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        // Initialize the subscription to ensure we have active subscribers before receiving messages
        // This is crucial to avoid FAIL_ZERO_SUBSCRIBER errors
        Flux<List<HotDealDto>> batchFlux = sink.asFlux()
                .onBackpressureBuffer(100, dropped -> log.warn("Dropped hot deal due to backpressure: {}", dropped))
                // Batch processing every 20 items or 10 seconds
                .windowTimeout(20, Duration.ofSeconds(10))
                .flatMap(window -> window.collectList());
        
        // Subscribe to the batch flux on a dedicated scheduler
        batchFlux.publishOn(Schedulers.boundedElastic())
                .subscribe(
                    this::processBatch,
                    error -> log.error("Error in reactive pipeline", error),
                    () -> log.info("Hot deal processor completed (this should not happen)")
                );
                
        // Log confirmation of subscription initialization
        log.info("Hot deal processor sink subscription initialized");
                
        // Periodically trim new hot deals streams to prevent unbounded growth
        Flux.interval(Duration.ofMinutes(1))
                .flatMap(tick -> trimNewHotDealsStreams())
                .subscribe(
                        trimmedCount -> {
                            if (trimmedCount > 0) {
                                log.debug("Trimmed {} entries from new hot deals streams", trimmedCount);
                            }
                        },
                        error -> log.error("Error trimming new hot deals streams", error)
                );
    }

    public void push(HotDealDto dto) {
        Sinks.EmitResult result = sink.tryEmitNext(dto);
        if (result.isFailure()) {
            if (result == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                // If there are no subscribers yet, try again with emitNext which will buffer the value
                // until a subscriber connects (since we're using a replay sink)
                try {
                    sink.emitNext(dto, Sinks.EmitFailureHandler.FAIL_FAST);
                    log.debug("Successfully emitted hot deal with forced emission: {} {}", dto.provider(), dto.link());
                } catch (Exception e) {
                    log.warn("Failed to force emit hot deal: {} {} . Error: {}", dto.provider(), dto.link(), e.getMessage());
                }
            } else {
                log.warn("Failed to emit hot deal: {} {} . Result: {}", dto.provider(), dto.link(), result);
            }
        } else {
            log.debug("Successfully emitted hot deal: {} {}", dto.provider(), dto.link());
        }
    }

    private void processBatch(List<HotDealDto> dtos) {
        if (dtos.isEmpty()) {
            return;
        }

        log.debug("Processing batch of {} hot deals", dtos.size());

        // Collect all links in the batch and query DB at once
        List<String> links = dtos.stream()
                .map(HotDealDto::link)
                .collect(Collectors.toList());

        List<HotDeal> existingDeals = repository.findAllByLinkIn(links);
        var existingLinks = existingDeals.stream()
                .map(HotDeal::getLink)
                .collect(Collectors.toSet());

        List<HotDeal> dealsToSave = dtos.stream()
                .filter(dto -> {
                    if (existingLinks.contains(dto.link())) {
                        log.debug("Duplicate found for link: {}", dto.link());
                        return false;
                    }
                    return true;
                })
                .map(dto -> {
                    try {
                        return HotDeal.fromDto(dto, hotdealService, providerCacheService);
                    } catch (Exception e) {
                        log.error("Error processing hot deal: {}. Error: {}", dto.link(), e.getMessage());
                        return null;
                    }
                })
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());

        if (!dealsToSave.isEmpty()) {
            repository.saveAll(dealsToSave);
            log.info("Saved {} new hot deals", dealsToSave.size());

            // Create DTO list from the saved hot deals for original DTOs that were saved
            List<HotDealDto> newHotDealDtos = dtos.stream()
                .filter(dto -> !existingLinks.contains(dto.link()))
                .collect(Collectors.toList());
                
            // Push new hot deals to the Redis stream for notification
            publishNewHotDealsToStream(newHotDealDtos);
        }
    }

    private void publishNewHotDealsToStream(List<HotDealDto> newDealDtos) {
        if (newDealDtos.isEmpty()) {
            return;
        }
            
        try {
            // Select a stream key for the new hot deals stream
            List<String> newHotDealsStreamKeys = streamConfig.getNewHotDealsStreamKeys();
            if (newHotDealsStreamKeys.isEmpty()) {
                log.error("No new hot deals stream keys configured");
                return;
            }
            
            // For each provider in the batch, publish individual hot deals to the new stream
            newDealDtos.forEach(dto -> {
                try {
                    String provider = dto.provider();
                    int partition = Math.abs(dto.link().hashCode() % streamConfig.getNewHotDealsPartitions());
                    String newStreamKey = String.format("%s:%d", streamConfig.getNewHotDealsKeyPrefix(), partition);
                    
                    // Encode a single hot deal as JSON
                    String dtoJsonData = objectMapper.writeValueAsString(dto);
                    String base64Data = java.util.Base64.getEncoder().encodeToString(
                            dtoJsonData.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    
                    // Add to new stream with the same key-value format as the original stream, but one message per hot deal
                    reactiveRedisTemplate.opsForStream()
                            .add(newStreamKey, Collections.singletonMap(provider, base64Data))
                            .subscribe(
                                    messageId -> log.debug("Published single hot deal to new stream {}, provider: {}, link: {}, message ID: {}",
                                            newStreamKey, provider, dto.link(), messageId),
                                    error -> log.error("Failed to publish hot deal to new stream {}, provider: {}, link: {}, error: {}",
                                            newStreamKey, provider, dto.link(), error.getMessage())
                            );
                } catch (Exception e) {
                    log.error("Error encoding hot deal data for {}, link: {}: {}", 
                            dto.provider(), dto.link(), e.getMessage());
                }
            });
                
            // Trim all new hot deals streams
            trimNewHotDealsStreams().subscribe(
                    trimmed -> {
                        if (trimmed > 0) {
                            log.debug("Trimmed {} total entries from new hot deals streams", trimmed);
                        }
                    },
                    error -> log.error("Failed to trim new hot deals streams: {}", error.getMessage())
            );
        } catch (Exception e) {
            log.error("Error publishing new hot deals to stream: {}", e.getMessage());
        }
    }
    
    /**
     * Trims all new hot deals streams to the configured maximum length.
     * @return The total number of entries trimmed
     */
    private reactor.core.publisher.Mono<Long> trimNewHotDealsStreams() {
        List<String> newHotDealsStreamKeys = streamConfig.getNewHotDealsStreamKeys();
        if (newHotDealsStreamKeys.isEmpty()) {
            return reactor.core.publisher.Mono.just(0L);
        }
        
        return reactor.core.publisher.Flux.fromIterable(newHotDealsStreamKeys)
                .flatMap(this::trimStreamToMaxLength)
                .reduce(0L, Long::sum);
    }
    
    /**
     * Trims a single stream to the configured maximum length.
     * @param streamKey The stream key to trim
     * @return The number of entries trimmed
     */
    private reactor.core.publisher.Mono<Long> trimStreamToMaxLength(String streamKey) {
        return reactiveRedisTemplate.opsForStream().trim(streamKey, NEW_HOTDEALS_STREAM_MAX_LENGTH, true)
                .onErrorResume(e -> {
                    log.warn("Failed to trim stream {}: {}", streamKey, e.getMessage());
                    return reactor.core.publisher.Mono.just(0L);
                });
    }
}

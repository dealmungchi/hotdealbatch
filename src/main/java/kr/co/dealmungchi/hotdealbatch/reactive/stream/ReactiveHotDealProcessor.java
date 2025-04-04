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
    private final Sinks.Many<HotDealDto> sink = Sinks.many().multicast().onBackpressureBuffer(100, false);

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        sink.asFlux()
                .onBackpressureBuffer(100, dropped -> log.warn("Dropped hot deal due to backpressure: {}", dropped))
                // Batch processing every 20 items or 10 seconds
                .windowTimeout(20, Duration.ofSeconds(10))
                .flatMap(window -> window.collectList())
                .publishOn(Schedulers.boundedElastic())
                .subscribe(this::processBatch, error -> log.error("Error in reactive pipeline", error));
                
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
            log.warn("Failed to emit hot deal: {} {} . Result: {}", dto.provider(), dto.link(), result);
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
            
            int partition = Math.abs(newDealDtos.hashCode() % streamConfig.getNewHotDealsPartitions());
            String newStreamKey = String.format("%s:%d", streamConfig.getNewHotDealsKeyPrefix(), partition);
            
            // For each provider in the batch, publish to the new stream with the same format
            newDealDtos.stream()
                .collect(Collectors.groupingBy(HotDealDto::provider))
                .forEach((provider, providerDtos) -> {
                    try {
                        String providerJsonData = objectMapper.writeValueAsString(providerDtos);
                        String providerBase64Data = java.util.Base64.getEncoder().encodeToString(
                                providerJsonData.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        
                        // Add to new stream with the same key-value format as the original stream
                        reactiveRedisTemplate.opsForStream()
                                .add(newStreamKey, Collections.singletonMap(provider, providerBase64Data))
                                .subscribe(
                                        messageId -> log.debug("Published new hot deals to new stream {}, provider: {}, message ID: {}",
                                                newStreamKey, provider, messageId),
                                        error -> log.error("Failed to publish new hot deals to new stream {}, provider: {}, error: {}",
                                                newStreamKey, provider, error.getMessage())
                                );
                    } catch (Exception e) {
                        log.error("Error encoding provider data for {}: {}", provider, e.getMessage());
                    }
                });
                
            // Trim the stream right after adding new entries
            trimStreamToMaxLength(newStreamKey).subscribe(
                    trimmed -> {
                        if (trimmed > 0) {
                            log.debug("Trimmed {} entries from stream {}", trimmed, newStreamKey);
                        }
                    },
                    error -> log.error("Failed to trim stream {}: {}", newStreamKey, error.getMessage())
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

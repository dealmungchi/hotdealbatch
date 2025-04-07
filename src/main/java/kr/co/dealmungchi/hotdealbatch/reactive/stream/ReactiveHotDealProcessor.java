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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reactive processor for hot deals.
 * Manages the reactive pipeline for processing hot deals with backpressure handling,
 * deduplication, persistence, and publishing to notification streams.
 */
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
    private final Sinks.Many<HotDealDto> sink = Sinks.many().replay().limit(100);

    /**
     * Initializes the reactive pipeline and periodic maintenance tasks.
     * Sets up batch processing and stream trimming.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        initializeReactivePipeline();
        scheduleStreamMaintenance();
        log.info("Hot deal processor initialized");
    }

    /**
     * Sets up the reactive data processing pipeline with batching and backpressure.
     */
    private void initializeReactivePipeline() {
        sink.asFlux()
            .onBackpressureBuffer(100, dropped -> log.warn("Dropped hot deal due to backpressure: {}", dropped))
            .windowTimeout(20, Duration.ofSeconds(10))
            .flatMap(window -> window.collectList())
            .publishOn(Schedulers.boundedElastic())
            .subscribe(
                this::processBatch,
                error -> log.error("Error in reactive pipeline", error),
                () -> log.info("Hot deal processor completed (this should not happen)")
            );
    }
    
    /**
     * Schedules periodic maintenance of Redis streams to prevent unbounded growth.
     */
    private void scheduleStreamMaintenance() {
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

    /**
     * Pushes a hot deal into the processing pipeline.
     * Handles backpressure and emission failures.
     *
     * @param dto The hot deal DTO to process
     */
    public void push(HotDealDto dto) {
        Sinks.EmitResult result = sink.tryEmitNext(dto);
        
        if (result.isFailure()) {
            handleEmitFailure(dto, result);
        } else {
            log.debug("Successfully emitted hot deal: {} {}", dto.provider(), dto.link());
        }
    }
    
    /**
     * Handles emission failures by attempting forced emission or logging errors.
     *
     * @param dto The hot deal DTO that failed to emit
     * @param result The emission result
     */
    private void handleEmitFailure(HotDealDto dto, Sinks.EmitResult result) {
        if (result == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            // If there are no subscribers yet, try forced emission
            try {
                sink.emitNext(dto, Sinks.EmitFailureHandler.FAIL_FAST);
                log.debug("Successfully emitted hot deal with forced emission: {} {}", dto.provider(), dto.link());
            } catch (Exception e) {
                log.warn("Failed to force emit hot deal: {} {} . Error: {}", dto.provider(), dto.link(), e.getMessage());
            }
        } else {
            log.warn("Failed to emit hot deal: {} {} . Result: {}", dto.provider(), dto.link(), result);
        }
    }

    /**
     * Processes a batch of hot deals.
     * Performs deduplication, saves new deals, and publishes to notification streams.
     *
     * @param dtos The list of hot deal DTOs to process
     */
    private void processBatch(List<HotDealDto> dtos) {
        if (dtos.isEmpty()) {
            return;
        }

        log.debug("Processing batch of {} hot deals", dtos.size());

        // Find new deals (those that don't exist in the database)
        Map<String, HotDealDto> dtosByLink = dtos.stream()
                .collect(Collectors.toMap(HotDealDto::link, Function.identity(), (existing, replacement) -> existing));
                
        List<String> allLinks = List.copyOf(dtosByLink.keySet());
        Set<String> existingLinks = repository.findAllByLinkIn(allLinks).stream()
                .map(HotDeal::getLink)
                .collect(Collectors.toSet());
                
        // Filter out existing deals and convert DTOs to entities
        List<HotDeal> dealsToSave = dtosByLink.entrySet().stream()
                .filter(entry -> !existingLinks.contains(entry.getKey()))
                .map(entry -> {
                    try {
                        return HotDeal.fromDto(entry.getValue(), hotdealService, providerCacheService);
                    } catch (Exception e) {
                        log.error("Error processing hot deal: {}. Error: {}", entry.getKey(), e.getMessage());
                        return null;
                    }
                })
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());

        // Save new deals and publish to notification stream
        if (!dealsToSave.isEmpty()) {
            saveAndPublishNewDeals(dealsToSave, dtosByLink, existingLinks);
        }
    }
    
    /**
     * Saves new hot deals to the database and publishes them to notification streams.
     *
     * @param dealsToSave List of new HotDeal entities to save
     * @param dtosByLink Map of all DTOs by their link
     * @param existingLinks Set of links that already exist in the database
     */
    private void saveAndPublishNewDeals(List<HotDeal> dealsToSave, Map<String, HotDealDto> dtosByLink, Set<String> existingLinks) {
        repository.saveAll(dealsToSave);
        log.info("Saved {} new hot deals", dealsToSave.size());

        // Create list of new DTOs for publishing
        List<HotDealDto> newDtos = dealsToSave.stream()
                .map(deal -> dtosByLink.get(deal.getLink()))
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());
                
        // Publish to notification stream
        publishNewHotDealsToStream(newDtos);
    }

    /**
     * Publishes new hot deals to Redis streams for notification.
     *
     * @param newDealDtos List of new hot deal DTOs to publish
     */
    private void publishNewHotDealsToStream(List<HotDealDto> newDealDtos) {
        if (newDealDtos.isEmpty() || streamConfig.getNewHotDealsStreamKeys().isEmpty()) {
            return;
        }
            
        try {
            newDealDtos.forEach(this::publishSingleHotDeal);
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
     * Publishes a single hot deal to the appropriate Redis stream.
     *
     * @param dto The hot deal DTO to publish
     */
    private void publishSingleHotDeal(HotDealDto dto) {
        try {
            String provider = dto.provider();
            int partition = Math.abs(dto.link().hashCode() % streamConfig.getNewHotDealsPartitions());
            String newStreamKey = String.format("%s:%d", streamConfig.getNewHotDealsKeyPrefix(), partition);
            
            // Encode hot deal as JSON and convert to Base64
            String dtoJsonData = objectMapper.writeValueAsString(dto);
            String base64Data = Base64.getEncoder().encodeToString(
                    dtoJsonData.getBytes(StandardCharsets.UTF_8));
            
            // Add to stream
            reactiveRedisTemplate.opsForStream()
                    .add(newStreamKey, Collections.singletonMap(provider, base64Data))
                    .subscribe(
                            messageId -> log.debug("Published hot deal to stream {}, provider: {}, link: {}, ID: {}",
                                    newStreamKey, provider, dto.link(), messageId),
                            error -> log.error("Failed to publish hot deal to stream {}: {}", newStreamKey, error.getMessage())
                    );
        } catch (Exception e) {
            log.error("Error encoding hot deal data for {}, link: {}: {}", 
                    dto.provider(), dto.link(), e.getMessage());
        }
    }
    
    /**
     * Trims all new hot deals streams to the configured maximum length.
     *
     * @return The total number of entries trimmed
     */
    private Mono<Long> trimNewHotDealsStreams() {
        List<String> newHotDealsStreamKeys = streamConfig.getNewHotDealsStreamKeys();
        if (newHotDealsStreamKeys.isEmpty()) {
            return Mono.just(0L);
        }
        
        return Flux.fromIterable(newHotDealsStreamKeys)
                .flatMap(this::trimStreamToMaxLength)
                .reduce(0L, Long::sum);
    }
    
    /**
     * Trims a single stream to the configured maximum length.
     *
     * @param streamKey The stream key to trim
     * @return The number of entries trimmed
     */
    private Mono<Long> trimStreamToMaxLength(String streamKey) {
        return reactiveRedisTemplate.opsForStream().trim(streamKey, NEW_HOTDEALS_STREAM_MAX_LENGTH, true)
                .onErrorResume(e -> {
                    log.warn("Failed to trim stream {}: {}", streamKey, e.getMessage());
                    return Mono.just(0L);
                });
    }
}

package kr.co.dealmungchi.hotdealbatch.reactive.stream

import com.fasterxml.jackson.databind.ObjectMapper
import kr.co.dealmungchi.hotdealbatch.domain.entity.HotDeal
import kr.co.dealmungchi.hotdealbatch.domain.repository.HotDealRepository
import kr.co.dealmungchi.hotdealbatch.domain.service.HotdealService
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto
import kr.co.dealmungchi.hotdealbatch.service.ProviderCacheService
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Base64

/**
 * Reactive processor for hot deals.
 * Manages the reactive pipeline for processing hot deals with backpressure handling,
 * deduplication, persistence, and publishing to notification streams.
 */
@Component
class ReactiveHotDealProcessor(
    private val repository: HotDealRepository,
    private val hotdealService: HotdealService,
    private val providerCacheService: ProviderCacheService,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val streamConfig: RedisStreamConfig,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(ReactiveHotDealProcessor::class.java)

    // Maximum length for new hot deals streams to prevent unbounded growth
    private val newHotdealsStreamMaxLength = 100
    
    // Sink: Buffers up to 100 items while managing backpressure
    private val sink: Sinks.Many<HotDealDto> = Sinks.many().replay().limit(1000)

    /**
     * Initializes the reactive pipeline and periodic maintenance tasks.
     * Sets up batch processing and stream trimming.
     */
    @EventListener(ApplicationReadyEvent::class)
    fun init() {
        initializeReactivePipeline()
        scheduleStreamMaintenance()
        log.info("Hot deal processor initialized")
    }

    /**
     * Sets up the reactive data processing pipeline with batching and backpressure.
     */
    private fun initializeReactivePipeline() {
        sink.asFlux()
            .onBackpressureBuffer(2000) { dropped -> log.warn("Dropped hot deal due to backpressure: {}", dropped) }
            .windowTimeout(100, Duration.ofSeconds(10))
            .flatMap { window -> window.collectList() }
            .publishOn(Schedulers.boundedElastic())
            .subscribe(
                { batch -> processBatch(batch) },
                { error -> log.error("Error in reactive pipeline", error) },
                { log.info("Hot deal processor completed (this should not happen)") }
            )
    }
    
    /**
     * Schedules periodic maintenance of Redis streams to prevent unbounded growth.
     */
    private fun scheduleStreamMaintenance() {
        Flux.interval(Duration.ofMinutes(1))
            .flatMap { trimNewHotDealsStreams() }
            .subscribe(
                { trimmedCount ->
                    if (trimmedCount > 0) {
                        log.debug("Trimmed {} entries from new hot deals streams", trimmedCount)
                    }
                },
                { error -> log.error("Error trimming new hot deals streams", error) }
            )
    }

    /**
     * Pushes a hot deal into the processing pipeline.
     * Handles backpressure and emission failures.
     *
     * @param dto The hot deal DTO to process
     */
    fun push(dto: HotDealDto) {
        val result = sink.tryEmitNext(dto)
        
        if (result.isFailure) {
            handleEmitFailure(dto, result)
        }
    }
    
    /**
     * Handles emission failures by attempting forced emission or logging errors.
     *
     * @param dto The hot deal DTO that failed to emit
     * @param result The emission result
     */
    private fun handleEmitFailure(dto: HotDealDto, result: Sinks.EmitResult) {
        if (result == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            // If there are no subscribers yet, try forced emission
            try {
                sink.emitNext(dto, Sinks.EmitFailureHandler.FAIL_FAST)
                log.debug("Successfully emitted hot deal with forced emission: {} {}", dto.provider, dto.link)
            } catch (e: Exception) {
                log.warn("Failed to force emit hot deal: {} {} . Error: {}", dto.provider, dto.link, e.message)
            }
        } else {
            log.warn("Failed to emit hot deal: {} {} . Result: {}", dto.provider, dto.link, result)
        }
    }

    /**
     * Processes a batch of hot deals.
     * Performs deduplication, saves new deals, and publishes to notification streams.
     *
     * @param dtos The list of hot deal DTOs to process
     */
    private fun processBatch(dtos: List<HotDealDto>) {
        if (dtos.isEmpty()) {
            return
        }

        log.debug("Processing batch of {} hot deals", dtos.size)

        // Find new deals (those that don't exist in the database)
        val dtosByLink = dtos.associateBy { it.link }
        val allLinks = dtosByLink.keys.toList()
        val existingLinks = repository.findAllByLinkIn(allLinks)
            .map { it.link }
            .toSet()
                
        // Filter out existing deals and convert DTOs to entities
        val dealsToSave = dtosByLink.entries
            .filter { !existingLinks.contains(it.key) }
            .mapNotNull { entry ->
                try {
                    HotDeal.fromDto(entry.value, hotdealService, providerCacheService)
                } catch (e: Exception) {
                    log.error("Error processing hot deal: {}. Error: {}", entry.key, e.message)
                    null
                }
            }

        // Save new deals and publish to notification stream
        if (dealsToSave.isNotEmpty()) {
            saveAndPublishNewDeals(dealsToSave, dtosByLink)
        } else {
            log.debug("No new hot deals to save")
        }
    }
    
    /**
     * Saves new hot deals to the database and publishes them to notification streams.
     *
     * @param dealsToSave List of new HotDeal entities to save
     * @param dtosByLink Map of all DTOs by their link
     */
    private fun saveAndPublishNewDeals(
        dealsToSave: List<HotDeal>,
        dtosByLink: Map<String, HotDealDto>
    ) {
        repository.saveAll(dealsToSave)
        log.info("Saved {} new hot deals", dealsToSave.size)

        // Create list of new DTOs for publishing
        val newDtos = dealsToSave
            .mapNotNull { deal -> dtosByLink[deal.link] }
                
        // Publish to notification stream
        publishNewHotDealsToStream(newDtos)
    }

    /**
     * Publishes new hot deals to Redis streams for notification.
     *
     * @param newDealDtos List of new hot deal DTOs to publish
     */
    private fun publishNewHotDealsToStream(newDealDtos: List<HotDealDto>) {
        if (newDealDtos.isEmpty() || streamConfig.newHotDealsStreamKeys.isEmpty()) {
            return
        }
            
        try {
            newDealDtos.forEach { publishSingleHotDeal(it) }
            trimNewHotDealsStreams().subscribe(
                { trimmed ->
                    if (trimmed > 0) {
                        log.debug("Trimmed {} total entries from new hot deals streams", trimmed)
                    }
                },
                { error -> log.error("Failed to trim new hot deals streams: {}", error.message) }
            )
        } catch (e: Exception) {
            log.error("Error publishing new hot deals to stream: {}", e.message)
        }
    }
    
    /**
     * Publishes a single hot deal to the appropriate Redis stream.
     *
     * @param dto The hot deal DTO to publish
     */
    private fun publishSingleHotDeal(dto: HotDealDto) {
        try {
            val provider = dto.provider
            val partition = Math.abs(dto.link.hashCode() % streamConfig.newHotDealsPartitions)
            val newStreamKey = "${streamConfig.newHotDealsKeyPrefix}:$partition"
            
            // Encode hot deal as JSON and convert to Base64
            val dtoJsonData = objectMapper.writeValueAsString(dto)
            val base64Data = Base64.getEncoder().encodeToString(
                dtoJsonData.toByteArray(StandardCharsets.UTF_8))
            
            // Add to stream
            reactiveRedisTemplate.opsForStream<String, String>()
                .add(newStreamKey, mapOf(provider to base64Data))
                .subscribe(
                    { messageId -> log.debug("Published hot deal to stream {}, provider: {}, link: {}, ID: {}",
                            newStreamKey, provider, dto.link, messageId) },
                    { error -> log.error("Failed to publish hot deal to stream {}: {}", newStreamKey, error.message) }
                )
        } catch (e: Exception) {
            log.error("Error encoding hot deal data for {}, link: {}: {}", 
                    dto.provider, dto.link, e.message)
        }
    }
    
    /**
     * Trims all new hot deals streams to the configured maximum length.
     *
     * @return The total number of entries trimmed
     */
    private fun trimNewHotDealsStreams(): Mono<Long> {
        val newHotDealsStreamKeys = streamConfig.newHotDealsStreamKeys
        if (newHotDealsStreamKeys.isEmpty()) {
            return Mono.just(0L)
        }
        
        return Flux.fromIterable(newHotDealsStreamKeys)
            .flatMap { trimStreamToMaxLength(it) }
            .reduce(0L) { a, b -> a + b }
    }
    
    /**
     * Trims a single stream to the configured maximum length.
     *
     * @param streamKey The stream key to trim
     * @return The number of entries trimmed
     */
    private fun trimStreamToMaxLength(streamKey: String): Mono<Long> {
        return reactiveRedisTemplate.opsForStream<String, String>()
            .trim(streamKey, newHotdealsStreamMaxLength.toLong(), true)
            .onErrorResume { e ->
                log.warn("Failed to trim stream {}: {}", streamKey, e.message)
                Mono.just(0L)
            }
    }
}
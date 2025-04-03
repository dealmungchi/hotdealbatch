package kr.co.dealmungchi.hotdealbatch.reactive;

import kr.co.dealmungchi.hotdealbatch.domain.entity.HotDeal;
import kr.co.dealmungchi.hotdealbatch.domain.repository.HotDealRepository;
import kr.co.dealmungchi.hotdealbatch.domain.service.HotdealService;
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
import kr.co.dealmungchi.hotdealbatch.service.ProviderCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveHotDealProcessor {

    private final HotDealRepository repository;
    private final HotdealService hotdealService;
    private final ProviderCacheService providerCacheService;

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
                .collect(Collectors.toList());

        if (!dealsToSave.isEmpty()) {
            repository.saveAll(dealsToSave);
            log.info("Saved {} new hot deals", dealsToSave.size());
        }
    }
}

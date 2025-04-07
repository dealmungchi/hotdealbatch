package kr.co.dealmungchi.hotdealbatch.service;

import kr.co.dealmungchi.hotdealbatch.domain.entity.Provider;
import kr.co.dealmungchi.hotdealbatch.domain.entity.ProviderType;
import kr.co.dealmungchi.hotdealbatch.domain.repository.ProviderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for caching and retrieving Provider entities.
 * Provides an in-memory cache to avoid frequent database lookups.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProviderCacheService {

    private final ProviderRepository providerRepository;
    private final Map<ProviderType, Provider> providerCache = new ConcurrentHashMap<>();

    /**
     * Initializes the cache on application startup.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        refreshCache();
    }

    /**
     * Refreshes the provider cache by loading all providers from the database.
     * This method is thread-safe.
     */
    public void refreshCache() {
        log.debug("Refreshing provider cache");
        providerCache.clear();
        
        providerRepository.findAll().forEach(provider -> 
            providerCache.put(provider.getProviderType(), provider));
            
        log.debug("Provider cache refreshed with {} entries", providerCache.size());
    }

    /**
     * Gets a provider by its type.
     * If the provider is not in the cache, it will be loaded from the database and cached.
     *
     * @param providerType The type of the provider
     * @return The provider entity
     * @throws IllegalArgumentException if the provider type is unknown
     */
    public Provider getProvider(ProviderType providerType) {
        return providerCache.computeIfAbsent(providerType, this::loadProviderFromDatabase);
    }

    /**
     * Loads a provider from the database.
     *
     * @param providerType The type of the provider
     * @return The provider entity
     * @throws IllegalArgumentException if the provider type is unknown
     */
    private Provider loadProviderFromDatabase(ProviderType providerType) {
        log.info("Provider for {} not found in cache, fetching from database", providerType);
        return providerRepository.findByProviderType(providerType)
                .orElseThrow(() -> new IllegalArgumentException("Unknown provider: " + providerType));
    }

    /**
     * Gets a provider by its string identifier.
     * The string is converted to a ProviderType before lookup.
     *
     * @param providerEn The string identifier of the provider
     * @return The provider entity
     * @throws IllegalArgumentException if the provider identifier is unknown
     */
    public Provider getProvider(String providerEn) {
        ProviderType providerType = ProviderType.from(providerEn);
        return getProvider(providerType);
    }
}
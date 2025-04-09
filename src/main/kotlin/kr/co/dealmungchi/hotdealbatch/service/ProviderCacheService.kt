package kr.co.dealmungchi.hotdealbatch.service

import kr.co.dealmungchi.hotdealbatch.domain.entity.Provider
import kr.co.dealmungchi.hotdealbatch.domain.entity.ProviderType
import kr.co.dealmungchi.hotdealbatch.domain.repository.ProviderRepository
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

/**
 * Service for caching and retrieving Provider entities.
 * Provides an in-memory cache to avoid frequent database lookups.
 */
@Service
class ProviderCacheService(private val providerRepository: ProviderRepository) {
    
    private val log = LoggerFactory.getLogger(ProviderCacheService::class.java)
    private val providerCache = ConcurrentHashMap<ProviderType, Provider>()

    /**
     * Initializes the cache on application startup.
     */
    @EventListener(ApplicationReadyEvent::class)
    fun init() {
        refreshCache()
    }

    /**
     * Refreshes the provider cache by loading all providers from the database.
     * This method is thread-safe.
     */
    fun refreshCache() {
        log.debug("Refreshing provider cache")
        providerCache.clear()
        
        providerRepository.findAll().forEach { provider -> 
            providerCache[provider.providerType] = provider
        }
            
        log.debug("Provider cache refreshed with {} entries", providerCache.size)
    }

    /**
     * Gets a provider by its type.
     * If the provider is not in the cache, it will be loaded from the database and cached.
     *
     * @param providerType The type of the provider
     * @return The provider entity
     * @throws IllegalArgumentException if the provider type is unknown
     */
    fun getProvider(providerType: ProviderType): Provider {
        return providerCache.computeIfAbsent(providerType) { loadProviderFromDatabase(it) }
    }

    /**
     * Loads a provider from the database.
     *
     * @param providerType The type of the provider
     * @return The provider entity
     * @throws IllegalArgumentException if the provider type is unknown
     */
    private fun loadProviderFromDatabase(providerType: ProviderType): Provider {
        log.info("Provider for {} not found in cache, fetching from database", providerType)
        return providerRepository.findByProviderType(providerType)
            .orElseThrow { IllegalArgumentException("Unknown provider: $providerType") }
    }

    /**
     * Gets a provider by its string identifier.
     * The string is converted to a ProviderType before lookup.
     *
     * @param providerEn The string identifier of the provider
     * @return The provider entity
     * @throws IllegalArgumentException if the provider identifier is unknown
     */
    fun getProvider(providerEn: String): Provider {
        val providerType = ProviderType.from(providerEn)
        return getProvider(providerType)
    }
}
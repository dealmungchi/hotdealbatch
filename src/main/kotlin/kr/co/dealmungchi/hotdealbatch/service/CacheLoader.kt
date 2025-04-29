package kr.co.dealmungchi.hotdealbatch.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kr.co.dealmungchi.hotdealbatch.domain.repository.ProviderRepository
import kr.co.dealmungchi.hotdealbatch.domain.repository.CategoryRepository
import kr.co.dealmungchi.hotdealbatch.domain.entity.ProviderType
import kr.co.dealmungchi.hotdealbatch.domain.entity.CategoryType
import kr.co.dealmungchi.hotdealbatch.domain.entity.Provider
import kr.co.dealmungchi.hotdealbatch.domain.entity.Category
import java.util.concurrent.ConcurrentHashMap
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener

/**
 * Service for loading and caching Provider and Category entities.
 */
@Service
class CacheLoader(
    private val providerRepository: ProviderRepository,
    private val categoryRepository: CategoryRepository
) {
    private val log = LoggerFactory.getLogger(CacheLoader::class.java)

    private val providerCache = ConcurrentHashMap<ProviderType, Provider>()
    private val categoryCache = ConcurrentHashMap<CategoryType, Category>()

    @EventListener(ApplicationReadyEvent::class)
    fun init() {
        refreshCaches()
    }
    
    fun refreshCaches() {
        loadProviders()
        loadCategories()
    }

    private fun loadProviders() {
        providerCache.clear()
        providerRepository.findAll().forEach { provider -> 
            providerCache[provider.providerType] = provider
        }
        log.info("Provider cache loaded with {} entries", providerCache.size)
    }

    private fun loadCategories() {
        categoryCache.clear()
        categoryRepository.findAll().forEach { category -> 
            categoryCache[category.categoryType] = category
        }
        log.info("Category cache loaded with {} entries", categoryCache.size)
    }

    fun getProvider(providerName: String): Provider {
        val providerType = ProviderType.from(providerName)
        return providerCache.computeIfAbsent(providerType) { fetchProvider(providerType) }
    }

    fun getCategory(categoryName: String): Category {
        val categoryType = CategoryType.fromName(categoryName)
        return categoryCache.computeIfAbsent(categoryType) { fetchCategory(categoryType) }
    }
    
    private fun fetchProvider(providerType: ProviderType): Provider {
        log.info("Provider for {} not found in cache, fetching from database", providerType)
        return providerRepository.findByProviderType(providerType)
            .orElseThrow { IllegalArgumentException("Unknown provider: $providerType") }
    }

    private fun fetchCategory(categoryType: CategoryType): Category {
        log.info("Category for {} not found in cache, fetching from database", categoryType)
        return categoryRepository.findByCategoryType(categoryType)
            .orElseThrow { IllegalArgumentException("Unknown category: $categoryType") }
    }
}
package kr.co.dealmungchi.hotdealbatch.domain.repository

import kr.co.dealmungchi.hotdealbatch.domain.entity.Provider
import kr.co.dealmungchi.hotdealbatch.domain.entity.ProviderType
import org.springframework.data.jpa.repository.JpaRepository
import java.util.Optional

/**
 * Repository for Provider entities.
 */
interface ProviderRepository : JpaRepository<Provider, Long> {
    /**
     * Find a provider by its type.
     *
     * @param providerType The type of the provider
     * @return An Optional containing the provider if found
     */
    fun findByProviderType(providerType: ProviderType): Optional<Provider>
}
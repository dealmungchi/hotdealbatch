package kr.co.dealmungchi.hotdealbatch.domain.repository

import kr.co.dealmungchi.hotdealbatch.domain.entity.HotDeal
import org.springframework.data.jpa.repository.JpaRepository

/**
 * Repository for HotDeal entities.
 */
interface HotDealRepository : JpaRepository<HotDeal, Long> {
    /**
     * Check if a hot deal with the given link exists.
     *
     * @param link The link to check
     * @return True if a hot deal with the link exists, false otherwise
     */
    fun existsByLink(link: String): Boolean
    
    /**
     * Find all hot deals with links in the given list.
     *
     * @param links The list of links to search for
     * @return A list of hot deals with the given links
     */
    fun findAllByLinkIn(links: List<String>): List<HotDeal>
}
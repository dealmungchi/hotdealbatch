package kr.co.dealmungchi.hotdealbatch.domain.repository

import kr.co.dealmungchi.hotdealbatch.domain.entity.Category
import kr.co.dealmungchi.hotdealbatch.domain.entity.CategoryType
import org.springframework.data.jpa.repository.JpaRepository
import java.util.Optional

/**
 * Repository for Category entities.
 */
interface CategoryRepository : JpaRepository<Category, Long> {
    /**
     * Find a category by its type.
     *
     * @param categoryType The type of the category
     * @return An Optional containing the category if found
     */
    fun findByCategoryType(categoryType: CategoryType): Optional<Category>
}
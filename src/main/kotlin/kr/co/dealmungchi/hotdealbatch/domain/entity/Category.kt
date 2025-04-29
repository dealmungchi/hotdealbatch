package kr.co.dealmungchi.hotdealbatch.domain.entity

import jakarta.persistence.*

/**
 * Entity representing a deal provider.
 */
@Entity
@Table(name = "categories", uniqueConstraints = [UniqueConstraint(columnNames = ["category_name"])])
class Category(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "category_type", nullable = false, unique = true)
    @Enumerated(value = EnumType.STRING)
    val categoryType: CategoryType
)
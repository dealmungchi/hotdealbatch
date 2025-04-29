package kr.co.dealmungchi.hotdealbatch.domain.entity

import jakarta.persistence.*
import kr.co.dealmungchi.hotdealbatch.domain.service.HotdealService
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto
import kr.co.dealmungchi.hotdealbatch.service.CacheLoader
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

/**
 * Entity representing a hot deal.
 */
@Entity
@Table(name = "hotdeals", uniqueConstraints = [UniqueConstraint(columnNames = ["link"])])
class HotDeal(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(nullable = false)
    val postId: String,

    @Column(nullable = false)
    val title: String,

    @Column(nullable = false, unique = true)
    val link: String,

    val price: String? = null,

    val thumbnailHash: String? = null,

    @Column(length = 2000)
    val thumbnailLink: String? = null,

    val postedAt: String? = null,

    val viewCount: Long = 0,

    @ManyToOne
    @JoinColumn(name = "provider_id", nullable = false)
    val provider: Provider,

    @ManyToOne
    @JoinColumn(name = "category_id", nullable = false)
    val category: Category
) : BaseTimeEntity() {

    companion object {
        /**
         * Creates a HotDeal entity from a HotDealDto.
         * Uploads the thumbnail if present.
         *
         * @param dto The DTO to convert
         * @param hotdealService The service to use for uploading thumbnails
         * @param cacheLoader The service to use for retrieving providers and categories
         * @return A new HotDeal entity
         */
        fun fromDto(
            dto: HotDealDto,
            hotdealService: HotdealService,
            cacheLoader: CacheLoader
        ): HotDeal {
            val provider = cacheLoader.getProvider(dto.provider)
            val category = cacheLoader.getCategory(dto.category)
            val thumbnailHash = dto.thumbnail?.let { thumbnailString ->
                val hashBytes = MessageDigest.getInstance("SHA-256")
                    .digest(thumbnailString.toByteArray(StandardCharsets.UTF_8))
                val hash = hashBytes.joinToString("") { byte -> "%02x".format(byte) }

                val thumbnailBytes = Base64.getDecoder().decode(thumbnailString)
                hotdealService.uploadThumbnail(hash, thumbnailBytes)

                hash
            }

            return HotDeal(
                postId = dto.postId,
                title = dto.title,
                link = dto.link,
                price = dto.price,
                thumbnailHash = thumbnailHash,
                thumbnailLink = dto.thumbnailLink,
                postedAt = dto.postedAt,
                provider = provider,
                category = category
            )
        }
    }
}

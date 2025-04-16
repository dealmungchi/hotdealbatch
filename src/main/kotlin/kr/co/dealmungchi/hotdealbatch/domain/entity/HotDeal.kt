package kr.co.dealmungchi.hotdealbatch.domain.entity

import jakarta.persistence.*
import kr.co.dealmungchi.hotdealbatch.domain.service.HotdealService
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto
import kr.co.dealmungchi.hotdealbatch.service.ProviderCacheService
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.security.MessageDigest

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
    val provider: Provider
) : BaseTimeEntity() {

    companion object {
        /**
         * Creates a HotDeal entity from a HotDealDto.
         * Uploads the thumbnail if present.
         *
         * @param dto The DTO to convert
         * @param hotdealService The service to use for uploading thumbnails
         * @param providerCacheService The service to use for retrieving providers
         * @return A new HotDeal entity
         */
        fun fromDto(
            dto: HotDealDto, 
            hotdealService: HotdealService, 
            providerCacheService: ProviderCacheService
        ): HotDeal {
            val provider = providerCacheService.getProvider(dto.provider)
            val thumbnailHash = dto.thumbnail?.let { it ->
                val hash = MessageDigest.getInstance("SHA-256")
                    .digest(it.toByteArray(StandardCharsets.UTF_8))
                    .joinToString("") { "%02x".format(it) }
                
                hotdealService.uploadThumbnail(
                    hash, Base64.getDecoder().decode(it)
                )
                
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
                provider = provider
            )
        }
    }
}
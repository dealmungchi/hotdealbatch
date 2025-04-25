package kr.co.dealmungchi.hotdealbatch.dto

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Data transfer object for hot deals.
 */
data class HotDealDto(
    val title: String,
    val link: String,
    val price: String?,
    val thumbnail: String?,
    
    @JsonProperty("thumbnail_link")
    val thumbnailLink: String?,
    
    @JsonProperty("id")
    val postId: String,
    
    @JsonProperty("posted_at")
    val postedAt: String?,

    @JsonProperty("category")
    val category: String?,
    
    val provider: String
)
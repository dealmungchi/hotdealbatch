package kr.co.dealmungchi.hotdealbatch.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record HotDealDto(
    String title,
    String link,
    String price,
    String thumbnail,
    
    @JsonProperty("id")
    String postId,
    
    @JsonProperty("posted_at")
    String postedAt,
    
    String provider
) {}


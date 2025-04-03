package kr.co.dealmungchi.hotdealbatch.domain.entity;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import com.google.common.hash.Hashing;

import jakarta.persistence.*;
import kr.co.dealmungchi.hotdealbatch.domain.service.HotdealService;
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
import kr.co.dealmungchi.hotdealbatch.service.ProviderCacheService;
import lombok.*;

@Entity
@Table(name = "hot_deals", uniqueConstraints = @UniqueConstraint(columnNames = "link"))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class HotDeal extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String postId;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false, unique = true)
    private String link;

    private String price;
    
    private String thumbnailHash;
    
    private String postedAt;

    @ManyToOne
    @JoinColumn(name = "provider_id", nullable = false)
    private Provider provider;
    
    public static HotDeal fromDto(HotDealDto dto, HotdealService hotdealService, ProviderCacheService providerCacheService) {
        Provider provider = providerCacheService.getProvider(dto.provider());

        HotDealBuilder builder = HotDeal.builder()
                .postId(dto.postId())
                .title(dto.title())
                .link(dto.link())
                .price(dto.price())
                .postedAt(dto.postedAt())
                .provider(provider);

        if (Objects.nonNull(dto.thumbnail())) {
            String thumbnailHash = Hashing.sha256()
                    .hashString(dto.thumbnail(), StandardCharsets.UTF_8)
                    .toString();
            builder.thumbnailHash(thumbnailHash);

            hotdealService.uploadThumbnail(
                thumbnailHash, Base64.getDecoder().decode(dto.thumbnail())
            );
        }

        return builder.build();
    }
}

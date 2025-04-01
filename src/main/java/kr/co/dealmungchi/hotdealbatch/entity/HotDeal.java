package kr.co.dealmungchi.hotdealbatch.entity;

import jakarta.persistence.*;
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
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
    
    @Lob
    private String thumbnail;
    
    private String postedAt;

    @ManyToOne
    @JoinColumn(name = "provider_id", nullable = false)
    private Provider provider;
    
    public static HotDeal fromDto(HotDealDto dto, Provider provider) {
        return HotDeal.builder()
            .postId(dto.postId())
            .title(dto.title())
            .link(dto.link())
            .price(dto.price())
            .thumbnail(dto.thumbnail())
            .postedAt(dto.postedAt())
            .provider(provider)
            .build();
    }
}

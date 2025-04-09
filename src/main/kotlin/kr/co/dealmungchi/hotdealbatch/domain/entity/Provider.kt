package kr.co.dealmungchi.hotdealbatch.domain.entity

import jakarta.persistence.*

/**
 * Entity representing a deal provider.
 */
@Entity
@Table(name = "providers")
class Provider(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "provider_type", nullable = false)
    @Enumerated(value = EnumType.STRING)
    val providerType: ProviderType
)
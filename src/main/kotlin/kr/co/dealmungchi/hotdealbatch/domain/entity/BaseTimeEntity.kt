package kr.co.dealmungchi.hotdealbatch.domain.entity

import jakarta.persistence.Column
import jakarta.persistence.EntityListeners
import jakarta.persistence.MappedSuperclass
import org.springframework.data.annotation.CreatedDate
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.jpa.domain.support.AuditingEntityListener
import java.time.LocalDateTime

/**
 * Base entity class that provides creation and update timestamp fields.
 */
@MappedSuperclass
@EntityListeners(AuditingEntityListener::class)
abstract class BaseTimeEntity {
    
    @Column(updatable = false)
    @CreatedDate
    protected var createdAt: LocalDateTime? = null

    @Column(insertable = false)
    @LastModifiedDate
    protected var updatedAt: LocalDateTime? = null
}
package kr.co.dealmungchi.hotdealbatch.entity;

import java.time.LocalDateTime;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import jakarta.persistence.Column;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.MappedSuperclass;
import lombok.Getter;

@Getter
@MappedSuperclass
@EntityListeners(AuditingEntityListener.class)
public abstract class BaseTimeEntity {
    
    @Column(updatable = false)
    @CreatedDate
    protected LocalDateTime createdAt;

    @Column(insertable = false)
    @LastModifiedDate
    protected LocalDateTime updatedAt;
}

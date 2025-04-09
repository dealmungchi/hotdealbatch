package kr.co.dealmungchi.hotdealbatch.reactive.stream

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.RedisSerializationContext

/**
 * Configuration for Redis.
 */
@Configuration
class RedisConfig {

    /**
     * Creates a reactive Redis template for string keys and values.
     *
     * @param connectionFactory The Redis connection factory
     * @return A reactive Redis template
     */
    @Bean
    fun reactiveRedisTemplate(
        connectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, String> = 
        ReactiveRedisTemplate(
            connectionFactory,
            RedisSerializationContext.string()
        )
}
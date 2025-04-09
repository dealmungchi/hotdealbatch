package kr.co.dealmungchi.hotdealbatch.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

/**
 * Configuration for Jackson ObjectMapper.
 */
@Configuration
class ObjectMapperConfig {
    
    /**
     * Creates a configured ObjectMapper bean for the application.
     * 
     * @return The configured ObjectMapper
     */
    @Bean
    @Primary
    fun objectMapper(): ObjectMapper {
        return ObjectMapper().apply {
            registerModule(JavaTimeModule())
            registerModule(KotlinModule.Builder().build())
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    }
}
package kr.co.dealmungchi.hotdealbatch.reactive.stream

import com.fasterxml.jackson.databind.ObjectMapper
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * Decodes Base64-encoded messages containing HotDealDto data in JSON format.
 */
@Component
class Base64MessageDecoder(private val objectMapper: ObjectMapper) {
    
    private val log = LoggerFactory.getLogger(Base64MessageDecoder::class.java)
    
    /**
     * Decodes a Base64-encoded string into a HotDealDto object.
     *
     * @param encodedData The Base64-encoded JSON data
     * @return A Mono of HotDealDto object
     */
    fun decode(encodedData: String): Mono<HotDealDto> {
        return Mono.fromCallable { toJson(encodedData) }
            .flatMap { parse(it) }
            .onErrorResume { e ->
                log.error("Error processing encoded data: {}", e.message)
                Mono.empty()
            }
    }
    
    /**
     * Decodes a Base64-encoded string to its original string content.
     *
     * @param encodedData The Base64-encoded data
     * @return The decoded string
     * @throws IllegalArgumentException If the input is not valid Base64
     */
    private fun toJson(encodedData: String): String {
        return try {
            String(Base64.getDecoder().decode(encodedData), StandardCharsets.UTF_8)
        } catch (e: IllegalArgumentException) {
            log.error("Failed to decode base64 message: {}", e.message)
            throw e
        }
    }
    
    /**
     * Parses JSON as a single HotDealDto object.
     *
     * @param json The JSON string to parse
     * @return A Mono of HotDealDto object
     */
    private fun parse(json: String): Mono<HotDealDto> {
        return try {
            val dto = objectMapper.readValue(json, HotDealDto::class.java)
            Mono.just(dto)
        } catch (e: Exception) {
            log.error("Failed to parse JSON as object: {} {}", json, e.message)
            Mono.empty()
        }
    }
}
package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Decodes Base64-encoded messages containing HotDealDto data in JSON format.
 * Supports both single object and array formats for backward compatibility.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class Base64MessageDecoder {
    
    private final ObjectMapper objectMapper;
    
    /**
     * Decodes a Base64-encoded string into a HotDealDto object.
     *
     * @param encodedData The Base64-encoded JSON data
     * @return A Mono of HotDealDto object
     */
    public Mono<HotDealDto> decode(String encodedData) {
        return Mono.fromCallable(() -> toJson(encodedData))
                .flatMap(this::parse)
                .onErrorResume(e -> {
                    log.error("Error processing encoded data: {}", encodedData, e);
                    return Mono.empty();
                });
    }
    
    /**
     * Decodes a Base64-encoded string to its original string content.
     *
     * @param encodedData The Base64-encoded data
     * @return The decoded string
     * @throws IllegalArgumentException If the input is not valid Base64
     */
    private String toJson(String encodedData) {
        try {
            return new String(Base64.getDecoder().decode(encodedData), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            log.error("Failed to decode base64 message: {}", encodedData, e);
            throw e;
        }
    }
    
    /**
     * Parses JSON as a single HotDealDto object.
     *
     * @param json The JSON string to parse
     * @return A Mono of HotDealDto object
     */
    private Mono<HotDealDto> parse(String json) {
        try {
            HotDealDto dto = objectMapper.readValue(json, HotDealDto.class);
            return Mono.just(dto);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse JSON as object: {}", json, e);
            return Mono.empty();
        }
    }
}

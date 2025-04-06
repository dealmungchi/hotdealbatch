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

@Slf4j
@Component
@RequiredArgsConstructor
public class Base64MessageDecoder {
    
    private final ObjectMapper objectMapper;
    
    public Flux<HotDealDto> decode(String encodedData) {
        return Mono.fromCallable(() -> {
            try {
                String json = new String(Base64.getDecoder().decode(encodedData), StandardCharsets.UTF_8);
                try {
                    // Try to parse as a single HotDealDto object first
                    HotDealDto dto = objectMapper.readValue(json, HotDealDto.class);
                    return new HotDealDto[] { dto };
                } catch (JsonProcessingException singleItemError) {
                    // If parsing as a single object fails, try as an array (for backward compatibility)
                    try {
                        return objectMapper.readValue(json, HotDealDto[].class);
                    } catch (JsonProcessingException arrayError) {
                        log.error("Failed to parse JSON as object or array after decoding: {}", encodedData, arrayError);
                        return new HotDealDto[0];
                    }
                }
            } catch (IllegalArgumentException e) {
                log.error("Failed to decode base64 message: {}", encodedData, e);
                return new HotDealDto[0];
            }
        }).flatMapMany(Flux::fromArray);
    }
}
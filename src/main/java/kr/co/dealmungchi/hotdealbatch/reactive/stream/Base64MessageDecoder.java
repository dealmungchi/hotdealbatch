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
                return objectMapper.readValue(json, HotDealDto[].class);
            } catch (IllegalArgumentException e) {
                log.error("Failed to decode base64 message: {}", encodedData, e);
                return new HotDealDto[0];
            } catch (JsonProcessingException e) {
                log.error("Failed to parse JSON after decoding: {}", encodedData, e);
                return new HotDealDto[0];
            }
        }).flatMapMany(Flux::fromArray);
    }
}
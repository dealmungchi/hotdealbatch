package kr.co.dealmungchi.hotdealbatch.stream;

import kr.co.dealmungchi.hotdealbatch.reactive.ReactiveHotDealProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class HotDealStreamMessageHandler implements StreamMessageHandler {
    
    private final Base64MessageDecoder decoder;
    private final ReactiveHotDealProcessor reactiveProcessor;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    
    // Set to track processed message IDs to prevent duplicate processing
    private static final String PROCESSED_MESSAGES_SET = "hotdeals:processed-messages";
    private static final Duration MESSAGE_ID_TTL = Duration.ofHours(24);
    
    @Override
    public void handleMessage(RedisStreamMessage message) {
        // For backward compatibility with tests, delegate to reactive implementation
        handleMessageReactive(message).block();
    }
    
    @Override
    public Mono<Void> handleMessageReactive(RedisStreamMessage message) {
        String messageId = message.getMessageId();
        
        return reactiveRedisTemplate.opsForSet().isMember(PROCESSED_MESSAGES_SET, messageId)
            .flatMap(isProcessed -> {
                if (Boolean.TRUE.equals(isProcessed)) {
                    log.debug("Message {} already processed, skipping", messageId);
                    return Mono.empty();
                }
                
                return decoder.decode(message.getData())
                    .doOnNext(reactiveProcessor::push)
                    .collectList()
                    .flatMap(dtos -> {
                        int size = dtos.size();
                        if (size == 0) {
                            log.warn("No DTOs found in message {}", messageId);
                            return Mono.empty();
                        }
                        
                        log.debug("Processed {} DTOs from message {}", size, messageId);
                        return reactiveRedisTemplate.opsForSet().add(PROCESSED_MESSAGES_SET, messageId)
                            .then(reactiveRedisTemplate.expire(PROCESSED_MESSAGES_SET, MESSAGE_ID_TTL))
                            .then();
                    });
            });
    }
}
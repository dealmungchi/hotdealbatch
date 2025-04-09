package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Handles Redis stream messages containing hot deal data.
 * Processes incoming messages by decoding them and forwarding to the reactive processor.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HotDealStreamMessageHandler implements StreamMessageHandler {
    
    private final Base64MessageDecoder decoder;
    private final ReactiveHotDealProcessor reactiveProcessor;
    
    /**
     * Synchronous message handler implementation.
     * Only used for backward compatibility with tests.
     * 
     * @param message The Redis stream message to process
     */
    @Override
    public void handleMessage(RedisStreamMessage message) {
        // For backward compatibility with tests, delegate to reactive implementation
        handleMessageReactive(message).block();
    }
    
    /**
     * Reactive message handler implementation.
     * Decodes the message data and forwards hot deals to the processor.
     * 
     * @param message The Redis stream message to process
     * @return A Mono that completes when message processing is finished
     */
    @Override
    public Mono<Void> handleMessageReactive(RedisStreamMessage message) {
        String messageId = message.getMessageId();
        
        return decoder.decode(message.getData()) // Ensure this returns Mono<HotDealDto>
            .flatMap(dto -> {
                reactiveProcessor.push(dto);
                log.debug("Processed DTO from message {}", messageId);
                return Mono.empty();
            });
    }
}
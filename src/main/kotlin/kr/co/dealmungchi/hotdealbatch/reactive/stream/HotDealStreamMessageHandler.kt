package kr.co.dealmungchi.hotdealbatch.reactive.stream

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * Handles Redis stream messages containing hot deal data.
 * Processes incoming messages by decoding them and forwarding to the reactive processor.
 */
@Component
class HotDealStreamMessageHandler(
    private val decoder: Base64MessageDecoder,
    private val reactiveProcessor: ReactiveHotDealProcessor
) : StreamMessageHandler {
    
    private val log = LoggerFactory.getLogger(HotDealStreamMessageHandler::class.java)
    
    /**
     * Synchronous message handler implementation.
     * Only used for backward compatibility with tests.
     * 
     * @param message The Redis stream message to process
     */
    override fun handleMessage(message: RedisStreamMessage) {
        // For backward compatibility with tests, delegate to reactive implementation
        handleMessageReactive(message).block()
    }
    
    /**
     * Reactive message handler implementation.
     * Decodes the message data and forwards hot deals to the processor.
     * 
     * @param message The Redis stream message to process
     * @return A Mono that completes when message processing is finished
     */
    override fun handleMessageReactive(message: RedisStreamMessage): Mono<Void> {
        val messageId = message.messageId
        
        return decoder.decode(message.data)
            .doOnNext { dto ->
                reactiveProcessor.push(dto)
            }
            .then()
    }
}
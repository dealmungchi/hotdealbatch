package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import reactor.core.publisher.Mono;

public interface StreamMessageHandler {
    void handleMessage(RedisStreamMessage message);
    
    Mono<Void> handleMessageReactive(RedisStreamMessage message);
}
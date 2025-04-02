package kr.co.dealmungchi.hotdealbatch.stream;

import reactor.core.publisher.Mono;

public interface StreamMessageHandler {
    void handleMessage(RedisStreamMessage message);
    
    Mono<Void> handleMessageReactive(RedisStreamMessage message);
}
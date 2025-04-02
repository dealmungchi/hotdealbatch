package kr.co.dealmungchi.hotdealbatch.stream;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RedisStreamMessage {
    private final String messageId;
    private final String streamKey;
    private final String provider;
    private final String data;
}
package kr.co.dealmungchi.hotdealbatch.stream;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
@Getter
public class RedisStreamConfig {

    @Value("${redis.stream.key:streamHotdeals}")
    private String streamKey;

    @Value("${redis.stream.partitions:1}")
    private int partitions;

    @Value("${redis.stream.consumer-group:hotdeals-batch-group}")
    private String consumerGroup;

    @Value("${redis.stream.consumer-prefix:consumer-}")
    private String consumerPrefix;

    @Value("${redis.stream.block-timeout:2000}")
    private long blockTimeout;

    @Value("${redis.stream.batch-size:10}")
    private int batchSize;

    @Value("${redis.stream.poll-timeout:100}")
    private long pollTimeout;

    @Value("${redis.stream.delivery-retry-count:3}")
    private int deliveryRetryCount;

    @Value("${redis.stream.message-claim-min-idle-time:30000}")
    private long messageClaimMinIdleTime;
    
    @Value("${redis.stream.backpressure-buffer-size:512}")
    private int backpressureBufferSize;

    public String getStreamKey(int partition) {
        return String.format("%s:%d", streamKey, partition % partitions);
    }

    public String getConsumerId() {
        return consumerPrefix + UUID.randomUUID();
    }
}
package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class RedisStreamConfig {

    @Value("${redis.stream.key-prefix:streamHotdeals}")
    private String streamKeyPrefix;

    @Value("${redis.stream.partitions:1}")
    private int partitions;

    @Value("${redis.stream.consumer-group:hotdeals-batch-group}")
    private String consumerGroup;

    @Value("${redis.stream.consumer:consumer-1}")
    private String consumer;

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

    @Value("${redis.stream.new-hotdeals-key-prefix:streamNewHotdeals}")
    private String newHotDealsKeyPrefix;

    @Value("${redis.stream.new-hotdeals-partitions:1}")
    private int newHotDealsPartitions;

    public List<String> getStreamKeys() {
        return IntStream.range(0, partitions)
                .mapToObj(partition -> String.format("%s:%d", streamKeyPrefix, partition % partitions))
                .collect(Collectors.toList());
    }
    
    public List<String> getNewHotDealsStreamKeys() {
        return IntStream.range(0, newHotDealsPartitions)
                .mapToObj(partition -> String.format("%s:%d", newHotDealsKeyPrefix, partition % newHotDealsPartitions))
                .collect(Collectors.toList());
    }
}
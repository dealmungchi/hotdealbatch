package kr.co.dealmungchi.hotdealbatch.reactive.stream;

import lombok.Getter;

import java.util.List;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Redis streams.
 * Provides properties and utility methods for working with Redis streams,
 * including partitioning for both input and output streams.
 */
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

    /**
     * Gets the list of input stream keys based on the configured number of partitions.
     * Each key follows the pattern {prefix}:{partition}.
     *
     * @return A list of input stream keys
     */
    public List<String> getStreamKeys() {
        return generatePartitionedKeys(streamKeyPrefix, partitions);
    }
    
    /**
     * Gets the list of output stream keys for new hot deals based on the configured number of partitions.
     * Each key follows the pattern {prefix}:{partition}.
     *
     * @return A list of output stream keys for new hot deals
     */
    public List<String> getNewHotDealsStreamKeys() {
        return generatePartitionedKeys(newHotDealsKeyPrefix, newHotDealsPartitions);
    }
    
    /**
     * Generates a list of partitioned keys following the pattern {prefix}:{partition}.
     *
     * @param keyPrefix The prefix for the keys
     * @param numPartitions The number of partitions to generate
     * @return A list of partitioned keys
     */
    private List<String> generatePartitionedKeys(String keyPrefix, int numPartitions) {
        return IntStream.range(0, numPartitions)
                .mapToObj(partition -> String.format("%s:%d", keyPrefix, partition))
                .toList();
    }
}
package kr.co.dealmungchi.hotdealbatch.reactive.stream

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration

/**
 * Configuration for Redis streams.
 * Provides properties and utility methods for working with Redis streams,
 * including partitioning for both input and output streams.
 */
@Configuration
class RedisStreamConfig(
    @Value("\${redis.stream.key-prefix:streamHotdeals}")
    val streamKeyPrefix: String,

    @Value("\${redis.stream.partitions:1}")
    val partitions: Int,

    @Value("\${redis.stream.consumer-group:hotdeals-batch-group}")
    val consumerGroup: String,

    @Value("\${redis.stream.consumer:consumer-1}")
    val consumer: String,

    @Value("\${redis.stream.block-timeout:2000}")
    val blockTimeout: Long,

    @Value("\${redis.stream.batch-size:10}")
    val batchSize: Int,

    @Value("\${redis.stream.poll-timeout:100}")
    val pollTimeout: Long,

    @Value("\${redis.stream.delivery-retry-count:3}")
    val deliveryRetryCount: Int,

    @Value("\${redis.stream.message-claim-min-idle-time:30000}")
    val messageClaimMinIdleTime: Long,

    @Value("\${redis.stream.backpressure-buffer-size:512}")
    val backpressureBufferSize: Int,

    @Value("\${redis.stream.new-hotdeals-key-prefix:streamNewHotdeals}")
    val newHotDealsKeyPrefix: String,

    @Value("\${redis.stream.new-hotdeals-partitions:1}")
    val newHotDealsPartitions: Int
) {

    /**
     * Gets the list of input stream keys based on the configured number of partitions.
     * Each key follows the pattern {prefix}:{partition}.
     *
     * @return A list of input stream keys
     */
    val streamKeys: List<String>
        get() = generatePartitionedKeys(streamKeyPrefix, partitions)
    
    /**
     * Gets the list of output stream keys for new hot deals based on the configured number of partitions.
     * Each key follows the pattern {prefix}:{partition}.
     *
     * @return A list of output stream keys for new hot deals
     */
    val newHotDealsStreamKeys: List<String>
        get() = generatePartitionedKeys(newHotDealsKeyPrefix, newHotDealsPartitions)
    
    /**
     * Generates a list of partitioned keys following the pattern {prefix}:{partition}.
     *
     * @param keyPrefix The prefix for the keys
     * @param numPartitions The number of partitions to generate
     * @return A list of partitioned keys
     */
    private fun generatePartitionedKeys(keyPrefix: String, numPartitions: Int): List<String> =
        (0 until numPartitions).map { partition -> "$keyPrefix:$partition" }
}
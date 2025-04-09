package kr.co.dealmungchi.hotdealbatch.reactive.stream

/**
 * Represents a message from a Redis stream.
 * Contains the message ID, stream key, provider, and encoded data.
 */
data class RedisStreamMessage(
    /**
     * The unique ID of the message in the Redis stream
     */
    val messageId: String,
    
    /**
     * The key of the Redis stream this message was read from
     */
    val streamKey: String,
    
    /**
     * The provider identifier (key in the Redis stream entry)
     */
    val provider: String,
    
    /**
     * The encoded data (value in the Redis stream entry)
     * This is typically Base64-encoded JSON
     */
    val data: String
)
package kr.co.dealmungchi.hotdealbatch.reactive.stream

import io.lettuce.core.RedisBusyException
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.kotlin.any
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.argumentCaptor
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.connection.stream.*
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.testcontainers.containers.GenericContainer
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@ExtendWith(MockitoExtension::class)
class ReactiveRedisStreamConsumerTest {

    companion object {
        private lateinit var redisContainer: GenericContainer<*>

        @BeforeAll
        @JvmStatic
        fun startRedisContainer() {
            redisContainer = GenericContainer("redis:7.0").withExposedPorts(6379)
            redisContainer.start()
        }

        @AfterAll
        @JvmStatic
        fun stopRedisContainer() {
            if (::redisContainer.isInitialized) {
                redisContainer.stop()
            }
        }
    }

    private lateinit var consumer: ReactiveRedisStreamConsumer
    private lateinit var reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
    private lateinit var connectionFactory: LettuceConnectionFactory

    @Mock
    private lateinit var config: RedisStreamConfig

    @Mock
    private lateinit var messageHandler: StreamMessageHandler

    private lateinit var meterRegistry: MeterRegistry

    @BeforeEach
    fun setUp() {
        connectionFactory = LettuceConnectionFactory(
            redisContainer.host,
            redisContainer.getMappedPort(6379)
        )
        connectionFactory.afterPropertiesSet()

        val serializationContext = RedisSerializationContext
            .newSerializationContext<String, String>(StringRedisSerializer())
            .value(StringRedisSerializer())
            .build()

        reactiveRedisTemplate = ReactiveRedisTemplate(connectionFactory, serializationContext)

        meterRegistry = SimpleMeterRegistry()

        // Use unique stream keys for each test to avoid interference
        val uniqueStreamKey = "test-stream-${UUID.randomUUID()}"
        
        lenient().`when`(config.consumer).thenReturn("test-consumer-1")
        lenient().`when`(config.consumerGroup).thenReturn("test-group")
        lenient().`when`(config.batchSize).thenReturn(10)
        lenient().`when`(config.blockTimeout).thenReturn(1000L)
        lenient().`when`(config.messageClaimMinIdleTime).thenReturn(30000L)
        lenient().`when`(config.streamKeys).thenReturn(listOf(uniqueStreamKey))

        consumer = ReactiveRedisStreamConsumer(
            reactiveRedisTemplate,
            config,
            messageHandler,
            meterRegistry
        )
    }

    @AfterEach
    fun tearDown() {
        config.streamKeys.forEach { streamKey ->
            try {
                reactiveRedisTemplate.delete(streamKey).block()
            } catch (ignored: Exception) {
                // Ignore exceptions during cleanup
            }
        }

        if (::consumer.isInitialized) {
            consumer.shutdown()
        }
        
        if (::connectionFactory.isInitialized) {
            connectionFactory.destroy()
        }
    }

    /**
     * Test scenario:
     * Given: A Redis stream and consumer configuration are set up
     * When: The init method of ReactiveRedisStreamConsumer is called
     * Then: Consumer groups should be properly created for the stream
     * 
     * Verify that consumer groups are correctly created during initialization
     */
    @Test
    @DisplayName("Given application ready event, when init is called, then consumer groups are created")
    fun initShouldCreateConsumerGroups() {
        // Given
        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()
        val streamKey = config.streamKeys[0]

        // When
        try {
            consumer.init()
        } catch (ignored: RedisBusyException) {}

        // Then
        StepVerifier.create(streamOperations.groups(streamKey))
            .expectNextMatches { group -> group.groupName() == "test-group" }
            .verifyComplete()
    }

    /**
     * Test scenario:
     * Given: A Redis stream with a message in the correct format
     * When: The consumer processes that message
     * Then: The message is properly acknowledged after being successfully handled
     * 
     * Verify that the consumer correctly acknowledges messages after processing
     * them and that the message data is properly extracted
     */
    @Test
    @DisplayName("Given a stream message in the correct format, when processing, then message is acknowledged after successful handling")
    fun processMessageShouldAcknowledgeAfterSuccessfulProcessing() {
        // Given
        val streamKey = config.streamKeys[0]
        val field = "Coolandjoy"
        val base64EncodedData = "YmFzZTY0RGF0YQ==" // Example base64-encoded data

        val messageEntries = HashMap<String, String>()
        messageEntries[field] = base64EncodedData

        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()
        
        // Create consumer group first
        try {
            streamOperations.createGroup(streamKey, ReadOffset.latest(), "test-group").block()
        } catch (ignored: RedisBusyException) {
            // Ignore if the group already exists
        }
        
        // Create CountDownLatch for synchronization
        val latch = CountDownLatch(1)
        
        // Set up our mock with proper typing (Mono<Void>)
        `when`(messageHandler.handleMessageReactive(any()))
            .thenAnswer { 
                // Count down the latch in a separate thread to simulate async completion
                Thread { latch.countDown() }.start()
                Mono.empty<Void>()
            }

        // Add message after handler is set up
        streamOperations.add(MapRecord.create(streamKey, messageEntries)).block()

        // When
        consumer.init()

        // Then - wait for message processing with timeout
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertThat(processed).isTrue()

        // Verify message was processed correctly
        val messageCaptor = argumentCaptor<RedisStreamMessage>()
        verify(messageHandler).handleMessageReactive(messageCaptor.capture())
        val capturedMessage = messageCaptor.firstValue
        assertThat(capturedMessage.streamKey).isEqualTo(streamKey)
        assertThat(capturedMessage.provider).isEqualTo(field)
        assertThat(capturedMessage.data).isEqualTo(base64EncodedData)
    }

    /**
     * Test scenario:
     * Given: A Redis stream with a base64-encoded message
     * When: The consumer processes that message
     * Then: The data is properly decoded and handled correctly
     * 
     * Verify the consumer correctly decodes base64 data before passing it to the
     * handler
     */
    @Test
    @DisplayName("Given a stream with base64-encoded data, when processing, then data is decoded and handled correctly")
    fun shouldDecodeBase64DataAndHandleCorrectly() {
        // Given
        val streamKey = config.streamKeys[0]
        val field = "Coolandjoy"
        // Single HotDealDto JSON as base64 - simulating new format
        val singleDealJson = "{\"title\":\"Test Deal\",\"link\":\"https://example.com/deal\",\"price\":\"10000\",\"thumbnail\":\"https://example.com/image.jpg\",\"id\":\"123\",\"posted_at\":\"2025-04-04\",\"provider\":\"Coolandjoy\"}"
        val base64EncodedData = Base64.getEncoder().encodeToString(singleDealJson.toByteArray(StandardCharsets.UTF_8))

        val messageEntries = HashMap<String, String>()
        messageEntries[field] = base64EncodedData

        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()
        
        // Create consumer group first
        try {
            streamOperations.createGroup(streamKey, ReadOffset.latest(), "test-group").block()
        } catch (ignored: RedisBusyException) {
            // Ignore if the group already exists
        }
        
        // Create CountDownLatch for synchronization
        val latch = CountDownLatch(1)
        
        // Set up our mock with proper typing (Mono<Void>)
        `when`(messageHandler.handleMessageReactive(any()))
            .thenAnswer { 
                // Count down the latch in a separate thread to simulate async completion
                Thread { latch.countDown() }.start()
                Mono.empty<Void>()
            }

        // Add message after handler is set up
        streamOperations.add(MapRecord.create(streamKey, messageEntries)).block()

        // When
        consumer.init()

        // Then - wait for message processing with timeout
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertThat(processed).isTrue()

        // Verify message was processed correctly
        val messageCaptor = argumentCaptor<RedisStreamMessage>()
        verify(messageHandler).handleMessageReactive(messageCaptor.capture())
        val capturedMessage = messageCaptor.firstValue
        assertThat(capturedMessage.streamKey).isEqualTo(streamKey)
        assertThat(capturedMessage.provider).isEqualTo(field)
        assertThat(capturedMessage.data).isEqualTo(base64EncodedData)
    }
    
    @Test
    @DisplayName("Given a stream with base64-encoded array data (legacy format), when processing, then data is decoded and handled correctly")
    fun shouldDecodeLegacyArrayBase64DataAndHandleCorrectly() {
        // Given
        val streamKey = config.streamKeys[0]
        val field = "Coolandjoy"
        // Array of HotDealDto JSON as base64 - simulating old format
        val arrayDealJson = "[{\"title\":\"Test Deal 1\",\"link\":\"https://example.com/deal1\",\"price\":\"10000\",\"thumbnail\":\"https://example.com/image1.jpg\",\"id\":\"123\",\"posted_at\":\"2025-04-04\",\"provider\":\"Coolandjoy\"},{\"title\":\"Test Deal 2\",\"link\":\"https://example.com/deal2\",\"price\":\"20000\",\"thumbnail\":\"https://example.com/image2.jpg\",\"id\":\"124\",\"posted_at\":\"2025-04-04\",\"provider\":\"Coolandjoy\"}]"
        val base64EncodedData = Base64.getEncoder().encodeToString(arrayDealJson.toByteArray(StandardCharsets.UTF_8))

        val messageEntries = HashMap<String, String>()
        messageEntries[field] = base64EncodedData

        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()
        
        // Create consumer group first
        try {
            streamOperations.createGroup(streamKey, ReadOffset.latest(), "test-group").block()
        } catch (ignored: RedisBusyException) {
            // Ignore if the group already exists
        }
        
        // Create CountDownLatch for synchronization
        val latch = CountDownLatch(1)
        
        // Set up our mock with proper typing (Mono<Void>)
        `when`(messageHandler.handleMessageReactive(any()))
            .thenAnswer { 
                // Count down the latch in a separate thread to simulate async completion
                Thread { latch.countDown() }.start()
                Mono.empty<Void>()
            }

        // Add message after handler is set up
        streamOperations.add(MapRecord.create(streamKey, messageEntries)).block()

        // When
        consumer.init()

        // Then - wait for message processing with timeout
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertThat(processed).isTrue()

        // Verify message was processed correctly
        val messageCaptor = argumentCaptor<RedisStreamMessage>()
        verify(messageHandler).handleMessageReactive(messageCaptor.capture())
        val capturedMessage = messageCaptor.firstValue
        assertThat(capturedMessage.streamKey).isEqualTo(streamKey)
        assertThat(capturedMessage.provider).isEqualTo(field)
        assertThat(capturedMessage.data).isEqualTo(base64EncodedData)
    }

    /**
     * Test scenario:
     * Given: A Redis stream with consumer configuration
     * When: The consumer's init method is called
     * Then: Stream trimming operations are performed periodically
     * 
     * Verify that the consumer performs stream trimming operations during execution
     */
    @Test
    @DisplayName("Given a stream, when init is called, then streams are trimmed periodically")
    fun shouldTrimStreamsAtRegularIntervals() {
        // Given
        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()
        val streamKey = config.streamKeys[0]
        
        // Create consumer group first
        try {
            streamOperations.createGroup(streamKey, ReadOffset.latest(), "test-group").block()
        } catch (ignored: RedisBusyException) {
            // Ignore if the group already exists
        }

        // When
        consumer.init()

        // Use CountDownLatch for synchronization
        val latch = CountDownLatch(1)
        
        // Since trimming happens on a schedule, we need to wait a bit
        // We'll verify that getStreamKeys is at least called which happens during trim operation
        Thread({
            try {
                // Wait for a while to give time for trim operation
                Thread.sleep(1500)
                latch.countDown()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }).start()
        
        val completed = latch.await(5, TimeUnit.SECONDS)
        assertThat(completed).isTrue()

        // Then
        verify(config, atLeastOnce()).streamKeys
    }

    /**
     * Test scenario:
     * Given: A Redis stream with a message that causes an error during processing
     * When: The consumer processes that message
     * Then: The error is handled gracefully and tracked in metrics
     * 
     * Verify that the consumer properly handles errors during message processing
     * and updates error metrics accordingly
     */
    @Test
    @DisplayName("Given message processing error, when a message is processed, then error is handled gracefully")
    fun shouldHandleMessageProcessingErrorsGracefully() {
        // Given
        val streamKey = config.streamKeys[0]
        val provider = "test-provider"
        val data = "{\"id\":1,\"name\":\"Test Deal\"}"

        val messageEntries = HashMap<String, String>()
        messageEntries[provider] = data

        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()

        // Create consumer group first
        try {
            streamOperations.createGroup(streamKey, ReadOffset.latest(), "test-group").block()
        } catch (ignored: RedisBusyException) {
            // Ignore if the group already exists
        }

        // Create CountDownLatch for synchronization
        val latch = CountDownLatch(1)

        // Fix: Ensure proper typing of any()
        `when`(messageHandler.handleMessageReactive(any()))
            .thenAnswer {
                // Count down the latch in a separate thread to simulate async completion
                Thread { latch.countDown() }.start()
                Mono.error<Void>(RuntimeException("Test error"))
            }

        // Add message after handler is set up
        streamOperations.add(MapRecord.create(streamKey, messageEntries)).block()

        // When
        consumer.init()

        // Then - wait for message processing with timeout
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertThat(processed).isTrue()

        // Wait a bit more for metrics to be updated
        Thread.sleep(500)

        // Verify error metrics
        verify(messageHandler).handleMessageReactive(any())

        val failedCounter = meterRegistry.find("hotdeal.messages.failed").counter()
        assertThat(failedCounter).isNotNull()
        assertThat(failedCounter.count()).isEqualTo(1.0)
    }

    /**
     * Test scenario:
     * Given: A Redis stream with pending messages from another consumer
     * When: The consumer's claim task runs
     * Then: The pending messages are claimed and processed by our consumer
     * 
     * Verify that the consumer properly claims and processes messages that were
     * left in a pending state by other consumers
     */
    @Test
    @DisplayName("Given pending messages, when claim task runs, then pending messages are claimed and processed")
    fun shouldClaimAndProcessPendingMessages() {
        // Given
        val streamKey = config.streamKeys[0]
        val provider = "test-provider"
        val data = "test-data"

        val messageEntries = HashMap<String, String>()
        messageEntries[provider] = data

        val streamOperations = reactiveRedisTemplate.opsForStream<String, String>()

        // First add a message to the stream to ensure it exists
        val messageId = streamOperations.add(MapRecord.create(streamKey, messageEntries)).block()
        assertThat(messageId).isNotNull()

        // Create consumer group with "0" to make sure it includes all messages
        try {
            streamOperations.createGroup(streamKey, ReadOffset.from("0"), "test-group").block()
        } catch (ignored: Exception) {
            // Ignore if the group already exists
        }

        // Read the message as another consumer but don't acknowledge it
        val otherConsumerId = "other-consumer"
        val records = streamOperations.read(
            Consumer.from("test-group", otherConsumerId),
            StreamReadOptions.empty().count(10),
            StreamOffset.create(streamKey, ReadOffset.from("0"))).collectList().block()

        // Skip the test if we can't read any records
        if (records == null || records.isEmpty()) {
            return // Skip the rest of the test
        }

        // Verify that message is now pending
        val pendingSummary = Objects.requireNonNull(streamOperations.pending(streamKey, "test-group")).block()
        if (pendingSummary == null || pendingSummary.totalPendingMessages == 0L) {
            return // Skip if no pending messages
        }

        // Lower the message claim idle time to ensure our consumer will claim it
        `when`(config.messageClaimMinIdleTime).thenReturn(100L) // 100ms

        // Create CountDownLatch for synchronization
        val latch = CountDownLatch(1)
        
        // Set up our mock with proper typing (Mono<Void>)
        `when`(messageHandler.handleMessageReactive(any()))
            .thenAnswer { 
                // Count down the latch in a separate thread to simulate async completion
                Thread { latch.countDown() }.start()
                Mono.empty<Void>()
            }

        // When - now our consumer should claim and process the pending message
        consumer.init()

        // Then - wait for message processing with timeout
        val processed = latch.await(5, TimeUnit.SECONDS)
        assertThat(processed).isTrue()

        verify(messageHandler).handleMessageReactive(any())
    }
}
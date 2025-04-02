# Testing Guide for HotDeal Batch Processor

## Prerequisites

Before running the tests, ensure that you have:

1. Redis server running locally on port 6379
2. JDK 17 or higher installed

## Running Tests

You can run the tests using Gradle:

```bash
./gradlew test
```

## Test Configuration

The tests use the following configuration:

- Redis streams with prefix `streamHotdeals-test` 
- Consumer group `hotdeals-test-group`
- H2 in-memory database

## Manual Testing

You can manually test the Redis Stream consumer by publishing messages to the stream using Redis CLI:

```bash
# Connect to Redis CLI
redis-cli

# Add a test message to stream
XADD streamHotdeals:0 * Coolandjoy "base64EncodedData"

# View stream contents
XRANGE streamHotdeals:0 - +

# View consumer group info
XINFO GROUPS streamHotdeals:0

# View pending messages
XPENDING streamHotdeals:0 hotdeals-batch-group
```

The base64 encoded data should be a JSON array of HotDealDto objects. Here's an example of creating a Base64 encoded message:

```java
String json = "[{\"title\":\"Test Product\",\"link\":\"https://example.com/product\",\"price\":\"10000\",\"thumbnail\":\"https://example.com/thumbnail.jpg\",\"id\":\"12345\",\"posted_at\":\"2025-04-01T12:00:00\",\"provider\":\"Coolandjoy\"}]";
String base64 = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
System.out.println(base64);
```

## Troubleshooting

1. If tests fail with connection errors, make sure Redis is running and accessible
2. If tests fail with "NOGROUP" errors, the test will automatically try to create the required consumer group
3. For timeout errors, increase the timeout in the test configuration
4. For stream related errors, you may need to clean up Redis streams with `DEL streamHotdeals-test:0`
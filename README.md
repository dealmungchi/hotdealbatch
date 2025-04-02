# HotDeal Batch Processor

A Spring Boot application that processes hot deals from multiple providers using Redis Streams.

## Overview

This application consumes messages from Redis Streams, processes them, and stores them in a database. It uses a consumer group approach with acknowledgment to ensure reliable message delivery.

## Features

- Redis Streams integration with consumer group processing
- Base64 message decoding
- Explicit acknowledgment (XACK) for processed messages
- Pending message recovery with XCLAIM
- Back-pressure control
- dotenv configuration
- Containerization with Docker

## Requirements

- JDK 17+
- Redis 7.0+
- MySQL 8.0+ (for production) or H2 (for development)

## Configuration

The application is configured through environment variables:

### Redis Stream Configuration

| Variable | Description | Default Value |
|----------|-------------|---------------|
| REDIS_STREAM_KEY | Base key for Redis Streams | streamHotdeals |
| REDIS_STREAM_PARTITIONS | Number of stream partitions | 1 |
| REDIS_STREAM_CONSUMER_GROUP | Consumer group name | hotdeals-batch-group |
| REDIS_STREAM_CONSUMER | consumer ID | consumer-1 |
| REDIS_STREAM_BLOCK_TIMEOUT | Blocking timeout in milliseconds | 2000 |
| REDIS_STREAM_BATCH_SIZE | Number of messages to read in a batch | 10 |
| REDIS_STREAM_POLL_TIMEOUT | Poll timeout in milliseconds | 100 |
| REDIS_STREAM_DELIVERY_RETRY_COUNT | Number of retries for message delivery | 3 |
| REDIS_STREAM_MESSAGE_CLAIM_MIN_IDLE_TIME | Minimum idle time for claiming pending messages (ms) | 30000 |

### Redis Connection Configuration

| Variable | Description | Default Value |
|----------|-------------|---------------|
| REDIS_HOST | Redis host | localhost |
| REDIS_PORT | Redis port | 6379 |

### Database Configuration

| Variable | Description | Default Value |
|----------|-------------|---------------|
| DB_HOST | Database host | localhost |
| DB_PORT | Database port | 3306 |
| DB_NAME | Database name | hotdeals |
| DB_USERNAME | Database username | - |
| DB_PASSWORD | Database password | - |

## Running in Development Mode

Use Docker Compose for local development:

```bash
docker-compose up
```

To run the application without Docker:

```bash
./gradlew bootRun
```

## Running in Production Mode

Build the Docker image:

```bash
docker build -t hotdealbatch:latest .
```

Run the container:

```bash
docker run -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e REDIS_HOST=your-redis-host \
  -e DB_HOST=your-db-host \
  -e DB_USERNAME=your-username \
  -e DB_PASSWORD=your-password \
  hotdealbatch:latest
```

## Message Format

Redis Stream messages are expected in the following format:

```
XADD streamHotdeals:0 * Coolandjoy "base64EncodedData"
```

Where:
- `streamHotdeals:0` is the stream key
- `Coolandjoy` is the provider name
- `base64EncodedData` is a Base64-encoded JSON array of hot deal objects

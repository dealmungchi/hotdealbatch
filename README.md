# HotDeal Batch Processor

A Spring Boot application that processes hot deals from multiple providers using Redis Streams.

## Overview

This application consumes messages from Redis Streams, processes them, and stores them in a database. It uses a consumer group approach with acknowledgment to ensure reliable message delivery.

## Features

- Redis Streams integration with consumer group processing
  - publush to new hotdeals stream when new hotdeals are found
- Thumbnail processing to hash and store to local or S3(optional) storage
- Save new hotdeals to database

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

### LocalFile Configuration

| Variable | Description | Default Value |
|----------|-------------|---------------|
| FILE_UPLOAD_DIR | store path on local | /usr/local/share/data |

### S3 Configuration

| Variable | Description | Default Value |
|----------|-------------|---------------|
| AWS_ACCESS_KEY_ID | aws access key | your_access_key_id |
| AWS_SECRET_ACCESS_KEY | aws secret key | your_secret_access_key |
| AWS_REGION | aws region | ap-northeast-2 |
| AWS_S3_BUCKET_NAME | aws s3 bucket name | hotdeals |

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
docker run -d \
  -p 8080:8080 \
  --name hotdealbatch \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e DB_HOST=your-db-host \
  -e DB_PORT=5432 \
  -e DB_NAME=your-db-name \
  -e DB_USERNAME=your-db-user \
  -e DB_PASSWORD=your-db-password \
  -e REDIS_HOST=your-redis-host \
  -e REDIS_PORT=6379 \
  -e REDIS_STREAM_KEY=your-stream-key \
  -e REDIS_STREAM_PARTITIONS=1 \
  -e REDIS_STREAM_CONSUMER_GROUP=your-group \
  -e REDIS_STREAM_CONSUMER=consumer-1 \
  -e REDIS_STREAM_BLOCK_TIMEOUT=1000 \
  -e REDIS_STREAM_BATCH_SIZE=10 \
  -e REDIS_STREAM_POLL_TIMEOUT=1000 \
  -e REDIS_STREAM_DELIVERY_RETRY_COUNT=3 \
  -e REDIS_STREAM_MESSAGE_CLAIM_MIN_IDLE_TIME=60000 \
  -e REDIS_STREAM_BACKPRESSURE_BUFFER_SIZE=100 \
  -e REDIS_STREAM_NEW_HOTDEALS_KEY=your-new-hotdeals-stream-key \
  -e REDIS_STREAM_NEW_HOTDEALS_PARTITIONS=1 \
  -e FILE_UPLOAD_DIR=/tmp/uploads \
  -e AWS_ACCESS_KEY_ID=your-access-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret-key \
  -e AWS_REGION=ap-northeast-2 \
  -e AWS_S3_BUCKET=your-bucket \
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

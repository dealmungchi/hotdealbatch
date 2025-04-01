# HotDeal Batch Processor

A batch processing system for collecting and processing hot deals from various sites.

## Project Overview

This project receives hot deal information via Redis, processes it using reactive programming, and stores it in a database.

## Key Features

- Hot deal data reception through Redis message subscription
- Reactive backpressure management and batch processing
- Duplicate handling before database storage
- Integrated management of hot deals from multiple sites (PPOMPPPU, CLIEN, etc.)

## Tech Stack

- Java 17
- Spring Boot 3.4.x
- Spring Data JPA
- Spring Batch
- Redis
- Project Reactor
- H2 Database (for development)
- MySQL (for production)
- Docker

## Development Setup

### Requirements

- JDK 17
- Gradle
- Redis server

### Running Locally

```bash
# Run in development environment
./gradlew bootRun --args='--spring.profiles.active=dev'

# Run in production environment
./gradlew bootRun --args='--spring.profiles.active=prod'
```

### Running with Docker

```bash
# Build the application
./gradlew clean build

# Run with Docker in development environment
docker-compose up -d
```

Environment variables can be set in the `.env` file or passed directly. Default values will be used if not specified. See `.env.example` for required variables.

## Configuration Files

- `application.yml`: Spring Boot configuration
  - Development environment (`dev`): Uses H2 in-memory database
  - Production environment (`prod`): Uses MySQL database

## Data Flow

1. Message reception from Redis "hotdeals" channel
2. Batch processing through ReactiveHotDealProcessor
3. Duplicate checking and storage in database

## Docker Configuration

- `Dockerfile`: Application container configuration
- `docker-compose.yml`: Configuration for application, MySQL, and Redis

## Project Structure

- `src/main/java/kr/co/dealmungchi/hotdealbatch/`
  - `batch/`: Spring Batch configuration
  - `dto/`: Data Transfer Objects
  - `entity/`: Database entities
  - `listener/`: Redis message listeners
  - `reactive/`: Reactive processing components
  - `repository/`: Data repositories
  - `service/`: Business logic services
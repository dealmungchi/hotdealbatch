FROM eclipse-temurin:17-jre-jammy as builder

WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .
COPY src src

RUN ./gradlew build -x test

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

ARG SPRING_PROFILES_ACTIVE=prod
ENV SPRING_PROFILES_ACTIVE=$SPRING_PROFILES_ACTIVE

# Redis Stream configuration with defaults
ENV REDIS_STREAM_KEY=streamHotdeals
ENV REDIS_STREAM_PARTITIONS=1
ENV REDIS_STREAM_CONSUMER_GROUP=hotdeals-batch-group
ENV REDIS_STREAM_CONSUMER_PREFIX=consumer-
ENV REDIS_STREAM_BLOCK_TIMEOUT=2000
ENV REDIS_STREAM_BATCH_SIZE=10
ENV REDIS_STREAM_POLL_TIMEOUT=100
ENV REDIS_STREAM_DELIVERY_RETRY_COUNT=3
ENV REDIS_STREAM_MESSAGE_CLAIM_MIN_IDLE_TIME=30000

# JVM configurations for container environment
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD wget -q --spider http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
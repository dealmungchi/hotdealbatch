FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY build/libs/*.jar app.jar

ARG SPRING_PROFILES_ACTIVE=prod
ENV SPRING_PROFILES_ACTIVE=$SPRING_PROFILES_ACTIVE

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
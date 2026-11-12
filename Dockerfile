# syntax=docker/dockerfile:1

FROM eclipse-temurin:27-jdk AS build
WORKDIR /workspace

# Dependencies first, so this layer is cached until the build files change.
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
RUN ./gradlew --no-daemon dependencies > /dev/null

COPY src ./src
RUN ./gradlew --no-daemon bootJar \
    && java -Djarmode=tools -jar build/libs/webhook-dispatcher.jar extract --layers --launcher --destination build/extracted

FROM eclipse-temurin:27-jre
RUN groupadd --system app && useradd --system --gid app --no-create-home app
WORKDIR /app

# One layer per Spring Boot layer: a code change only rebuilds the last one.
COPY --from=build /workspace/build/extracted/dependencies/ ./
COPY --from=build /workspace/build/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/build/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/build/extracted/application/ ./

USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "org.springframework.boot.loader.launch.JarLauncher"]

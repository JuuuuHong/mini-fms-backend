# 빌드 단계: Gradle wrapper 로 실행 JAR 을 만든다(테스트는 CI·로컬에서 따로 돌린다).
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
RUN ./gradlew --no-daemon dependencies > /dev/null
COPY src src
RUN ./gradlew --no-daemon bootJar -x test

# 실행 단계
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 1001 fms
COPY --from=build /workspace/build/libs/mini-fms-backend-*.jar app.jar
USER fms
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]

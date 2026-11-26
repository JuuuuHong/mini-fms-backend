plugins {
    java
    id("org.springframework.boot") version "3.5.16"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.minifms"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation("org.springframework.kafka:spring-kafka")
    // 돌발상황 관제 이력 검색(Elasticsearch Java API Client, 버전은 Spring Boot 가 관리)
    implementation("co.elastic.clients:elasticsearch-java")
    implementation("org.elasticsearch.client:elasticsearch-rest-client")
    // ClickHouse JDBC(HTTP 8123). Spring Boot 가 버전을 관리하지 않으므로 직접 고정한다.
    implementation("com.clickhouse:clickhouse-jdbc:0.9.9")
    // 라이드·기기 상태·결제 아웃박스(PostgreSQL). 상태 변경과 결제 요청 적재를 한 트랜잭션으로 묶는다.
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")
    // 대여·반납 분산 락. Spring 의 Redis 연결(Lettuce)은 그대로 두고 락에만 쓴다(스타터는 연결 팩토리를 바꾸므로 쓰지 않는다).
    // 3.x 마지막 줄(Netty 4.1)이라 Spring Boot 3.5 가 관리하는 Netty 와 맞는다.
    implementation("org.redisson:redisson:3.52.0")
    // 반납 구역 판정(다각형 포함 여부 + STRtree 공간 색인)
    implementation("org.locationtech.jts:jts-core:1.20.0")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:clickhouse")
    testImplementation("org.testcontainers:elasticsearch")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}

// 로컬 실행은 local 프로필(개발용 JWT 키·토큰 발급 API)로 띄운다.
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    systemProperty("spring.profiles.active", System.getenv("SPRING_PROFILES_ACTIVE") ?: "local")
}

tasks.test {
    useJUnitPlatform()
    // 통합 테스트는 컨테이너(Kafka·Redis·ClickHouse)를 한 번 띄워 모든 테스트 클래스가 공유한다.
    maxParallelForks = 1
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

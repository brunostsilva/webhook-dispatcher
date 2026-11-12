import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.boot)
}

group = "io.github.brunostsilva"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(27)
    }
}

kotlin {
    compilerOptions {
        // Kotlin 2.4 emits bytecode up to Java 26; the application is built and run on Java 27.
        jvmTarget = JvmTarget.JVM_26
        freeCompilerArgs.addAll("-Xjdk-release=26")
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 26
}

// Warnings fail the build for production code; tests may use unchecked casts on JSON maps.
tasks.named<KotlinCompile>("compileKotlin") {
    compilerOptions.allWarningsAsErrors = true
}

// Log4j2 instead of Spring Boot's default Logback, on every classpath (LoggingBackendTest checks it).
configurations.configureEach {
    exclude(group = "org.springframework.boot", module = "spring-boot-starter-logging")
}

dependencies {
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-webclient")
    implementation("org.springframework.boot:spring-boot-starter-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-log4j2")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation(libs.resilience4j.circuitbreaker)
    implementation(libs.resilience4j.micrometer)
    implementation("org.flywaydb:flyway-database-postgresql")

    runtimeOnly("org.postgresql:r2dbc-postgresql")
    runtimeOnly("org.postgresql:postgresql") // Flyway migrates over JDBC; the application uses R2DBC
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly(libs.disruptor) // Log4j2 async loggers (see log4j2.component.properties)

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-r2dbc")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = TestExceptionFormat.FULL
    }
}

// Logging is configured once per JVM, by the first application context, so the JSON logs test runs
// in a JVM of its own, where the json-logs profile is the one that configures it.
val jsonLogsTest = "*JsonLogsIntegrationTest"

tasks.test {
    filter { excludeTestsMatching(jsonLogsTest) }
}

val jsonLogsTestTask = tasks.register<Test>("jsonLogsTest") {
    description = "Runs the JSON logs test in its own JVM."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter { includeTestsMatching(jsonLogsTest) }
    shouldRunAfter(tasks.test)
}

tasks.check {
    dependsOn(jsonLogsTestTask)
}

tasks.bootJar {
    archiveFileName = "webhook-dispatcher.jar"
}

tasks.jar {
    enabled = false
}

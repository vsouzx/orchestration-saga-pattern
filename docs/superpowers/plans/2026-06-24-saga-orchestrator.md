# Saga Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate from choreographed saga to orchestrated saga by creating a new `saga-orchestrator` service (Kotlin/Spring Boot) and converting existing services from event publishers/consumers to command handlers that reply to the orchestrator.

**Architecture:** The orchestrator owns a state machine that drives the saga forward via Kafka command/reply. Each participant service (orders, payments, inventory) becomes a pure command handler that receives commands and sends replies. The orchestrator persists saga state + history in PostgreSQL and uses the transactional outbox pattern (same as inventory-service) to publish commands.

**Tech Stack:** Kotlin 2.1.10, Spring Boot 3.4.5, Java 21, PostgreSQL 17, Spring Kafka, Spring Data JPA, OpenTelemetry, Logstash Logback encoder, Jackson

## Global Constraints

- Spring Boot 3.4.5 (same as inventory-service)
- Java 21, Kotlin 2.1.10
- PostgreSQL 17 for orchestrator DB (dedicated instance, port 5433)
- Hexagonal architecture (ports & adapters, same package structure as inventory-service)
- Transactional outbox pattern with `@Scheduled` polling relay (same as inventory-service)
- All Kafka message payloads are JSON strings
- Trace context propagated via `traceparent` Kafka header (W3C format)
- Structured JSON logging via Logstash Logback encoder + OTel MDC appender
- Base package: `br.com.souza.saga_orchestrator`
- Go services use `segmentio/kafka-go`, `github.com/spf13/viper` config, `github.com/gin-gonic/gin` HTTP, `go.uber.org/zap` logging
- Go services use `config.yaml` + env var overrides via viper

---

### Task 1: Saga Orchestrator — Project Scaffolding, Domain Models & Schema

**Files:**
- Create: `saga-orchestrator/pom.xml`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/SagaOrchestratorApplication.kt`
- Create: `saga-orchestrator/src/main/resources/application.yaml`
- Create: `saga-orchestrator/src/main/resources/logback-spring.xml`
- Create: `saga-orchestrator/INIT.sql`
- Create: `saga-orchestrator/Dockerfile`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/model/Saga.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/model/SagaStep.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/model/SagaHistory.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/model/OutboxEvent.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/model/OutboxEventStatus.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/model/ReplyStatus.kt`

**Interfaces:**
- Consumes: nothing (first task)
- Produces:
  - `Saga(id: String, orderId: String, currentStep: SagaStep, payload: String, createdAt: LocalDateTime, updatedAt: LocalDateTime)`
  - `SagaHistory(id: String, sagaId: String, step: SagaStep, status: String, reason: String?, createdAt: LocalDateTime)`
  - `SagaStep` enum: `STARTED, RESERVING_STOCK, PROCESSING_PAYMENT, CONFIRMING_ORDER, CONFIRMING_RESERVATION, COMPLETED, RELEASING_STOCK, CANCELING_ORDER, FAILED`
  - `OutboxEvent(id: String?, aggregateId: String, aggregateType: String, eventType: String, topic: String, payload: String, traceParent: String?, status: OutboxEventStatus, retriesCount: Int, maxRetries: Int, createdAt: LocalDateTime, sentAt: LocalDateTime?, lockedAt: LocalDateTime?)`
  - `OutboxEventStatus` enum: `PENDING, PROCESSING, SENT, FAILED, DEAD_LETTER`
  - `ReplyStatus` enum: `SUCCESS, FAILURE, CREATED`

- [ ] **Step 1: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>
	<parent>
		<groupId>org.springframework.boot</groupId>
		<artifactId>spring-boot-starter-parent</artifactId>
		<version>3.4.5</version>
		<relativePath/>
	</parent>
	<groupId>br.com.souza</groupId>
	<artifactId>saga-orchestrator</artifactId>
	<version>0.0.1-SNAPSHOT</version>
	<name>saga-orchestrator</name>
	<description>Saga orchestrator for orders saga</description>
	<properties>
		<java.version>21</java.version>
		<kotlin.version>2.1.10</kotlin.version>
	</properties>
	<dependencyManagement>
		<dependencies>
			<dependency>
				<groupId>io.opentelemetry.instrumentation</groupId>
				<artifactId>opentelemetry-instrumentation-bom</artifactId>
				<version>2.12.0</version>
				<type>pom</type>
				<scope>import</scope>
			</dependency>
		</dependencies>
	</dependencyManagement>
	<dependencies>
		<dependency>
			<groupId>org.springframework.boot</groupId>
			<artifactId>spring-boot-starter-data-jpa</artifactId>
		</dependency>
		<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka</artifactId>
		</dependency>
		<dependency>
			<groupId>org.springframework.boot</groupId>
			<artifactId>spring-boot-starter-web</artifactId>
		</dependency>
		<dependency>
			<groupId>com.fasterxml.jackson.module</groupId>
			<artifactId>jackson-module-kotlin</artifactId>
		</dependency>
		<dependency>
			<groupId>org.jetbrains.kotlin</groupId>
			<artifactId>kotlin-reflect</artifactId>
		</dependency>
		<dependency>
			<groupId>org.jetbrains.kotlin</groupId>
			<artifactId>kotlin-stdlib</artifactId>
		</dependency>
		<dependency>
			<groupId>org.postgresql</groupId>
			<artifactId>postgresql</artifactId>
			<scope>runtime</scope>
		</dependency>
		<dependency>
			<groupId>net.logstash.logback</groupId>
			<artifactId>logstash-logback-encoder</artifactId>
			<version>8.1</version>
		</dependency>
		<dependency>
			<groupId>io.opentelemetry.instrumentation</groupId>
			<artifactId>opentelemetry-spring-boot-starter</artifactId>
		</dependency>
		<dependency>
			<groupId>io.opentelemetry.instrumentation</groupId>
			<artifactId>opentelemetry-logback-mdc-1.0</artifactId>
			<version>2.12.0-alpha</version>
		</dependency>
		<dependency>
			<groupId>org.springframework.boot</groupId>
			<artifactId>spring-boot-starter-test</artifactId>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka-test</artifactId>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.jetbrains.kotlin</groupId>
			<artifactId>kotlin-test-junit5</artifactId>
			<scope>test</scope>
		</dependency>
		<dependency>
			<groupId>org.mockito.kotlin</groupId>
			<artifactId>mockito-kotlin</artifactId>
			<version>5.4.0</version>
			<scope>test</scope>
		</dependency>
	</dependencies>

	<build>
		<sourceDirectory>${project.basedir}/src/main/kotlin</sourceDirectory>
		<testSourceDirectory>${project.basedir}/src/test/kotlin</testSourceDirectory>
		<plugins>
			<plugin>
				<groupId>org.springframework.boot</groupId>
				<artifactId>spring-boot-maven-plugin</artifactId>
			</plugin>
			<plugin>
				<groupId>org.jetbrains.kotlin</groupId>
				<artifactId>kotlin-maven-plugin</artifactId>
				<configuration>
					<args>
						<arg>-Xjsr305=strict</arg>
					</args>
					<compilerPlugins>
						<plugin>spring</plugin>
						<plugin>jpa</plugin>
						<plugin>all-open</plugin>
					</compilerPlugins>
					<pluginOptions>
						<option>all-open:annotation=jakarta.persistence.Entity</option>
						<option>all-open:annotation=jakarta.persistence.MappedSuperclass</option>
						<option>all-open:annotation=jakarta.persistence.Embeddable</option>
					</pluginOptions>
				</configuration>
				<dependencies>
					<dependency>
						<groupId>org.jetbrains.kotlin</groupId>
						<artifactId>kotlin-maven-allopen</artifactId>
						<version>${kotlin.version}</version>
					</dependency>
					<dependency>
						<groupId>org.jetbrains.kotlin</groupId>
						<artifactId>kotlin-maven-noarg</artifactId>
						<version>${kotlin.version}</version>
					</dependency>
				</dependencies>
			</plugin>
		</plugins>
	</build>
</project>
```

- [ ] **Step 2: Create `SagaOrchestratorApplication.kt`**

```kotlin
package br.com.souza.saga_orchestrator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class SagaOrchestratorApplication

fun main(args: Array<String>) {
    runApplication<SagaOrchestratorApplication>(*args)
}
```

- [ ] **Step 3: Create `application.yaml`**

```yaml
spring:
  application:
    name: saga-orchestrator

  datasource:
    url: jdbc:postgresql://localhost:5433/saga_db
    username: saga
    password: saga
    driver-class-name: org.postgresql.Driver

  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
    properties:
      hibernate:
        format_sql: false
        dialect: org.hibernate.dialect.PostgreSQLDialect

  kafka:
    bootstrap-servers: localhost:29092

server:
  port: 8084

otel:
  sdk:
    disabled: false
  exporter:
    otlp:
      endpoint: http://localhost:4318
  traces:
    exporter: otlp
  metrics:
    exporter: none
  logs:
    exporter: otlp

saga:
  timeout-minutes: 5
```

- [ ] **Step 4: Create `logback-spring.xml`**

Copy the exact same file from inventory-service:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <include resource="org/springframework/boot/logging/logback/defaults.xml"/>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
            <providers>
                <timestamp/>
                <logLevel/>
                <loggerName/>
                <threadName/>
                <message/>
                <mdc/>
                <arguments/>
                <stackTrace/>
            </providers>
        </encoder>
    </appender>

    <appender name="MDC" class="io.opentelemetry.instrumentation.logback.mdc.v1_0.OpenTelemetryAppender">
        <appender-ref ref="CONSOLE"/>
    </appender>

    <appender name="OTEL" class="io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender">
        <captureExperimentalAttributes>false</captureExperimentalAttributes>
        <captureMdcAttributes>*</captureMdcAttributes>
    </appender>

    <root level="INFO">
        <appender-ref ref="MDC"/>
        <appender-ref ref="OTEL"/>
    </root>
</configuration>
```

- [ ] **Step 5: Create `INIT.sql`**

```sql
CREATE TABLE sagas (
    id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    current_step VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE saga_history (
    id VARCHAR(36) PRIMARY KEY,
    saga_id VARCHAR(36) NOT NULL REFERENCES sagas(id),
    step VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_saga_history_saga_id ON saga_history(saga_id);

CREATE TABLE outbox_events (
    id VARCHAR(36) PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    trace_parent VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retries_count INT DEFAULT 0,
    max_retries INT DEFAULT 5,
    created_at TIMESTAMP DEFAULT NOW(),
    sent_at TIMESTAMP,
    locked_at TIMESTAMP
);

CREATE INDEX idx_outbox_events_pending ON outbox_events(status, created_at) WHERE status IN ('PENDING', 'FAILED');
```

- [ ] **Step 6: Create `Dockerfile`**

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/*.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 7: Create domain models**

`SagaStep.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.domain.model

enum class SagaStep {
    STARTED,
    RESERVING_STOCK,
    PROCESSING_PAYMENT,
    CONFIRMING_ORDER,
    CONFIRMING_RESERVATION,
    COMPLETED,
    RELEASING_STOCK,
    CANCELING_ORDER,
    FAILED
}
```

`ReplyStatus.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.domain.model

enum class ReplyStatus {
    SUCCESS,
    FAILURE,
    CREATED
}
```

`Saga.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.domain.model

import java.time.LocalDateTime

data class Saga(
    val id: String,
    val orderId: String,
    val currentStep: SagaStep,
    val payload: String,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val updatedAt: LocalDateTime = LocalDateTime.now()
)
```

`SagaHistory.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.domain.model

import java.time.LocalDateTime

data class SagaHistory(
    val id: String,
    val sagaId: String,
    val step: SagaStep,
    val status: String,
    val reason: String? = null,
    val createdAt: LocalDateTime = LocalDateTime.now()
)
```

`OutboxEventStatus.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.domain.model

enum class OutboxEventStatus {
    PENDING,
    PROCESSING,
    SENT,
    FAILED,
    DEAD_LETTER
}
```

`OutboxEvent.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.domain.model

import java.time.LocalDateTime

data class OutboxEvent(
    val id: String? = null,
    val aggregateId: String,
    val aggregateType: String,
    val eventType: String,
    val topic: String,
    val payload: String,
    val traceParent: String?,
    val status: OutboxEventStatus = OutboxEventStatus.PENDING,
    val retriesCount: Int = 0,
    val maxRetries: Int = 5,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val sentAt: LocalDateTime? = null,
    val lockedAt: LocalDateTime? = null
)
```

- [ ] **Step 8: Verify project compiles**

Run: `cd saga-orchestrator && ./mvnw compile -q`
Expected: BUILD SUCCESS (you need to first copy `mvnw` and `.mvn/` from inventory-service or run `mvn wrapper:wrapper`)

- [ ] **Step 9: Commit**

```bash
git add saga-orchestrator/
git commit -m "feat(orchestrator): scaffold project with domain models and DB schema"
```

---

### Task 2: Saga Orchestrator — State Machine (Pure Logic, No I/O)

**Files:**
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/service/SagaStateMachine.kt`
- Create: `saga-orchestrator/src/test/kotlin/br/com/souza/saga_orchestrator/application/domain/service/SagaStateMachineTest.kt`

**Interfaces:**
- Consumes: `Saga`, `SagaStep`, `ReplyStatus` from Task 1
- Produces: `SagaStateMachine.transition(currentStep: SagaStep, replyStatus: ReplyStatus): Transition`
  - `Transition(nextStep: SagaStep, commandTopic: String?, commandEventType: String?)`

- [ ] **Step 1: Write the failing test**

```kotlin
package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class SagaStateMachineTest {

    private val stateMachine = SagaStateMachine()

    // Happy path transitions
    @Test
    fun `STARTED + SUCCESS should transition to RESERVING_STOCK`() {
        val transition = stateMachine.transition(SagaStep.STARTED, ReplyStatus.CREATED)
        assertEquals(SagaStep.RESERVING_STOCK, transition.nextStep)
        assertEquals("inventory.commands.reserve-stock", transition.commandTopic)
        assertEquals("RESERVE_STOCK", transition.commandEventType)
    }

    @Test
    fun `RESERVING_STOCK + SUCCESS should transition to PROCESSING_PAYMENT`() {
        val transition = stateMachine.transition(SagaStep.RESERVING_STOCK, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.PROCESSING_PAYMENT, transition.nextStep)
        assertEquals("payments.commands.process-payment", transition.commandTopic)
        assertEquals("PROCESS_PAYMENT", transition.commandEventType)
    }

    @Test
    fun `PROCESSING_PAYMENT + SUCCESS should transition to CONFIRMING_ORDER`() {
        val transition = stateMachine.transition(SagaStep.PROCESSING_PAYMENT, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CONFIRMING_ORDER, transition.nextStep)
        assertEquals("orders.commands.confirm-order", transition.commandTopic)
        assertEquals("CONFIRM_ORDER", transition.commandEventType)
    }

    @Test
    fun `CONFIRMING_ORDER + SUCCESS should transition to CONFIRMING_RESERVATION`() {
        val transition = stateMachine.transition(SagaStep.CONFIRMING_ORDER, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CONFIRMING_RESERVATION, transition.nextStep)
        assertEquals("inventory.commands.confirm-reservation", transition.commandTopic)
        assertEquals("CONFIRM_RESERVATION", transition.commandEventType)
    }

    @Test
    fun `CONFIRMING_RESERVATION + SUCCESS should transition to COMPLETED`() {
        val transition = stateMachine.transition(SagaStep.CONFIRMING_RESERVATION, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.COMPLETED, transition.nextStep)
        assertEquals(null, transition.commandTopic)
    }

    // Compensation transitions
    @Test
    fun `RESERVING_STOCK + FAILURE should transition to CANCELING_ORDER`() {
        val transition = stateMachine.transition(SagaStep.RESERVING_STOCK, ReplyStatus.FAILURE)
        assertEquals(SagaStep.CANCELING_ORDER, transition.nextStep)
        assertEquals("orders.commands.cancel-order", transition.commandTopic)
        assertEquals("CANCEL_ORDER", transition.commandEventType)
    }

    @Test
    fun `PROCESSING_PAYMENT + FAILURE should transition to RELEASING_STOCK`() {
        val transition = stateMachine.transition(SagaStep.PROCESSING_PAYMENT, ReplyStatus.FAILURE)
        assertEquals(SagaStep.RELEASING_STOCK, transition.nextStep)
        assertEquals("inventory.commands.release-stock", transition.commandTopic)
        assertEquals("RELEASE_STOCK", transition.commandEventType)
    }

    @Test
    fun `RELEASING_STOCK + SUCCESS should transition to CANCELING_ORDER`() {
        val transition = stateMachine.transition(SagaStep.RELEASING_STOCK, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CANCELING_ORDER, transition.nextStep)
        assertEquals("orders.commands.cancel-order", transition.commandTopic)
        assertEquals("CANCEL_ORDER", transition.commandEventType)
    }

    @Test
    fun `CANCELING_ORDER + SUCCESS should transition to FAILED`() {
        val transition = stateMachine.transition(SagaStep.CANCELING_ORDER, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.FAILED, transition.nextStep)
        assertEquals(null, transition.commandTopic)
    }

    // Terminal states
    @Test
    fun `COMPLETED is terminal and should throw`() {
        assertThrows<IllegalStateException> {
            stateMachine.transition(SagaStep.COMPLETED, ReplyStatus.SUCCESS)
        }
    }

    @Test
    fun `FAILED is terminal and should throw`() {
        assertThrows<IllegalStateException> {
            stateMachine.transition(SagaStep.FAILED, ReplyStatus.SUCCESS)
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd saga-orchestrator && ./mvnw test -Dtest=SagaStateMachineTest -q`
Expected: FAIL — `SagaStateMachine` class does not exist

- [ ] **Step 3: Write minimal implementation**

```kotlin
package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

data class Transition(
    val nextStep: SagaStep,
    val commandTopic: String?,
    val commandEventType: String?
)

class SagaStateMachine {

    private data class TransitionKey(val step: SagaStep, val status: ReplyStatus)

    private val transitions = mapOf(
        // Happy path
        TransitionKey(SagaStep.STARTED, ReplyStatus.CREATED) to Transition(
            SagaStep.RESERVING_STOCK, "inventory.commands.reserve-stock", "RESERVE_STOCK"
        ),
        TransitionKey(SagaStep.RESERVING_STOCK, ReplyStatus.SUCCESS) to Transition(
            SagaStep.PROCESSING_PAYMENT, "payments.commands.process-payment", "PROCESS_PAYMENT"
        ),
        TransitionKey(SagaStep.PROCESSING_PAYMENT, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CONFIRMING_ORDER, "orders.commands.confirm-order", "CONFIRM_ORDER"
        ),
        TransitionKey(SagaStep.CONFIRMING_ORDER, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CONFIRMING_RESERVATION, "inventory.commands.confirm-reservation", "CONFIRM_RESERVATION"
        ),
        TransitionKey(SagaStep.CONFIRMING_RESERVATION, ReplyStatus.SUCCESS) to Transition(
            SagaStep.COMPLETED, null, null
        ),

        // Compensation
        TransitionKey(SagaStep.RESERVING_STOCK, ReplyStatus.FAILURE) to Transition(
            SagaStep.CANCELING_ORDER, "orders.commands.cancel-order", "CANCEL_ORDER"
        ),
        TransitionKey(SagaStep.PROCESSING_PAYMENT, ReplyStatus.FAILURE) to Transition(
            SagaStep.RELEASING_STOCK, "inventory.commands.release-stock", "RELEASE_STOCK"
        ),
        TransitionKey(SagaStep.RELEASING_STOCK, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CANCELING_ORDER, "orders.commands.cancel-order", "CANCEL_ORDER"
        ),
        TransitionKey(SagaStep.CANCELING_ORDER, ReplyStatus.SUCCESS) to Transition(
            SagaStep.FAILED, null, null
        )
    )

    fun transition(currentStep: SagaStep, replyStatus: ReplyStatus): Transition {
        if (currentStep == SagaStep.COMPLETED || currentStep == SagaStep.FAILED) {
            throw IllegalStateException("Cannot transition from terminal state: $currentStep")
        }

        return transitions[TransitionKey(currentStep, replyStatus)]
            ?: throw IllegalStateException("No transition defined for step=$currentStep, status=$replyStatus")
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd saga-orchestrator && ./mvnw test -Dtest=SagaStateMachineTest -q`
Expected: All 11 tests PASS

- [ ] **Step 5: Commit**

```bash
git add saga-orchestrator/src/
git commit -m "feat(orchestrator): implement saga state machine with full transition map"
```

---

### Task 3: Saga Orchestrator — Persistence Layer (Ports, JPA Entities, Adapters)

**Files:**
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/ports/out/SagaRepositoryPort.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/ports/out/SagaHistoryRepositoryPort.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/ports/out/OutboxEventRepositoryPort.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/models/SagaJpaEntity.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/models/SagaHistoryJpaEntity.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/repository/SagaJpaRepository.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/repository/SagaHistoryJpaRepository.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/mappers/SagaMapper.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/mappers/SagaHistoryMapper.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/SagaRepositoryAdapter.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/SagaHistoryRepositoryAdapter.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/relay/models/OutboxEventJpaEntity.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/relay/repository/OutboxEventJpaRepository.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/relay/mappers/OutboxEventMapper.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/relay/OutboxEventRepositoryAdapter.kt`

**Interfaces:**
- Consumes: `Saga`, `SagaHistory`, `SagaStep`, `OutboxEvent`, `OutboxEventStatus` from Task 1
- Produces:
  - `SagaRepositoryPort`: `save(saga: Saga): Saga`, `findById(id: String): Saga?`, `findByOrderId(orderId: String): Saga?`, `findTimedOutSagas(olderThan: LocalDateTime): List<Saga>`
  - `SagaHistoryRepositoryPort`: `save(history: SagaHistory): SagaHistory`, `findBySagaId(sagaId: String): List<SagaHistory>`
  - `OutboxEventRepositoryPort`: `save(event: OutboxEvent): OutboxEvent`, `findPendingEvents(limit: Int): List<OutboxEvent>`, `lockEvents(ids: List<String>): Int`, `markAsSent(id: String)`, `markAsFailed(id: String)`, `markAsDeadLetter(id: String)`

- [ ] **Step 1: Create output ports**

`SagaRepositoryPort.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.Saga
import java.time.LocalDateTime

interface SagaRepositoryPort {
    fun save(saga: Saga): Saga
    fun findById(id: String): Saga?
    fun findByOrderId(orderId: String): Saga?
    fun findTimedOutSagas(olderThan: LocalDateTime): List<Saga>
}
```

`SagaHistoryRepositoryPort.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.SagaHistory

interface SagaHistoryRepositoryPort {
    fun save(history: SagaHistory): SagaHistory
    fun findBySagaId(sagaId: String): List<SagaHistory>
}
```

`OutboxEventRepositoryPort.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.OutboxEvent

interface OutboxEventRepositoryPort {
    fun save(event: OutboxEvent): OutboxEvent
    fun findPendingEvents(limit: Int): List<OutboxEvent>
    fun lockEvents(ids: List<String>): Int
    fun markAsSent(id: String)
    fun markAsFailed(id: String)
    fun markAsDeadLetter(id: String)
}
```

- [ ] **Step 2: Create JPA entities**

`SagaJpaEntity.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga.models

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "sagas")
data class SagaJpaEntity(
    @Id
    val id: String = "",

    @Column(name = "order_id", nullable = false)
    val orderId: String = "",

    @Column(name = "current_step", nullable = false)
    val currentStep: String = "",

    @Column(nullable = false, columnDefinition = "JSONB")
    val payload: String = "",

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime = LocalDateTime.now()
)
```

`SagaHistoryJpaEntity.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga.models

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "saga_history")
data class SagaHistoryJpaEntity(
    @Id
    val id: String = "",

    @Column(name = "saga_id", nullable = false)
    val sagaId: String = "",

    @Column(nullable = false)
    val step: String = "",

    @Column(nullable = false)
    val status: String = "",

    val reason: String? = null,

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now()
)
```

`OutboxEventJpaEntity.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.relay.models

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "outbox_events")
data class OutboxEventJpaEntity(
    @Id
    val id: String = "",

    @Column(name = "aggregate_id", nullable = false)
    val aggregateId: String = "",

    @Column(name = "aggregate_type", nullable = false)
    val aggregateType: String = "",

    @Column(name = "event_type", nullable = false)
    val eventType: String = "",

    @Column(nullable = false)
    val topic: String = "",

    @Column(nullable = false, columnDefinition = "JSONB")
    val payload: String = "",

    @Column(name = "trace_parent")
    val traceParent: String? = null,

    @Column(nullable = false)
    val status: String = "PENDING",

    @Column(name = "retries_count", nullable = false)
    val retriesCount: Int = 0,

    @Column(name = "max_retries", nullable = false)
    val maxRetries: Int = 5,

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(name = "sent_at")
    val sentAt: LocalDateTime? = null,

    @Column(name = "locked_at")
    val lockedAt: LocalDateTime? = null
)
```

- [ ] **Step 3: Create JPA repositories**

`SagaJpaRepository.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga.repository

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaJpaEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.LocalDateTime

interface SagaJpaRepository : JpaRepository<SagaJpaEntity, String> {
    fun findByOrderId(orderId: String): SagaJpaEntity?

    @Query("""
        SELECT s FROM SagaJpaEntity s
        WHERE s.updatedAt < :olderThan
        AND s.currentStep NOT IN ('COMPLETED', 'FAILED')
    """)
    fun findTimedOutSagas(@Param("olderThan") olderThan: LocalDateTime): List<SagaJpaEntity>
}
```

`SagaHistoryJpaRepository.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga.repository

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaHistoryJpaEntity
import org.springframework.data.jpa.repository.JpaRepository

interface SagaHistoryJpaRepository : JpaRepository<SagaHistoryJpaEntity, String> {
    fun findBySagaIdOrderByCreatedAtAsc(sagaId: String): List<SagaHistoryJpaEntity>
}
```

`OutboxEventJpaRepository.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.relay.repository

import br.com.souza.saga_orchestrator.adapter.out.relay.models.OutboxEventJpaEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.LocalDateTime

interface OutboxEventJpaRepository : JpaRepository<OutboxEventJpaEntity, String> {

    @Query("""
        SELECT e FROM OutboxEventJpaEntity e
        WHERE (e.status = 'PENDING' OR e.status = 'FAILED')
        AND (e.lockedAt IS NULL OR e.lockedAt < :expiredBefore)
        ORDER BY e.createdAt ASC
        LIMIT :limit
    """)
    fun findPendingEvents(
        @Param("limit") limit: Int,
        @Param("expiredBefore") expiredBefore: LocalDateTime
    ): List<OutboxEventJpaEntity>

    @Modifying
    @Query("""
        UPDATE OutboxEventJpaEntity e
        SET e.status = 'PROCESSING', e.lockedAt = :now
        WHERE e.id IN :ids AND (e.status = 'PENDING' OR e.status = 'FAILED')
    """)
    fun lockEvents(@Param("ids") ids: List<String>, @Param("now") now: LocalDateTime): Int

    @Modifying
    @Query("""
        UPDATE OutboxEventJpaEntity e
        SET e.status = 'SENT', e.sentAt = :now, e.lockedAt = NULL
        WHERE e.id = :id
    """)
    fun markAsSent(@Param("id") id: String, @Param("now") now: LocalDateTime)

    @Modifying
    @Query("""
        UPDATE OutboxEventJpaEntity e
        SET e.status = 'FAILED', e.lockedAt = NULL, e.retriesCount = e.retriesCount + 1
        WHERE e.id = :id
    """)
    fun markAsFailed(@Param("id") id: String)

    @Modifying
    @Query("""
        UPDATE OutboxEventJpaEntity e
        SET e.status = 'DEAD_LETTER', e.lockedAt = NULL
        WHERE e.id = :id
    """)
    fun markAsDeadLetter(@Param("id") id: String)
}
```

- [ ] **Step 4: Create mappers**

`SagaMapper.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga.mappers

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaJpaEntity
import br.com.souza.saga_orchestrator.application.domain.model.Saga
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

fun Saga.toJpaEntity() = SagaJpaEntity(
    id = id,
    orderId = orderId,
    currentStep = currentStep.name,
    payload = payload,
    createdAt = createdAt,
    updatedAt = updatedAt
)

fun SagaJpaEntity.toDomain() = Saga(
    id = id,
    orderId = orderId,
    currentStep = SagaStep.valueOf(currentStep),
    payload = payload,
    createdAt = createdAt,
    updatedAt = updatedAt
)
```

`SagaHistoryMapper.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga.mappers

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaHistoryJpaEntity
import br.com.souza.saga_orchestrator.application.domain.model.SagaHistory
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

fun SagaHistory.toJpaEntity() = SagaHistoryJpaEntity(
    id = id,
    sagaId = sagaId,
    step = step.name,
    status = status,
    reason = reason,
    createdAt = createdAt
)

fun SagaHistoryJpaEntity.toDomain() = SagaHistory(
    id = id,
    sagaId = sagaId,
    step = SagaStep.valueOf(step),
    status = status,
    reason = reason,
    createdAt = createdAt
)
```

`OutboxEventMapper.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.relay.mappers

import br.com.souza.saga_orchestrator.adapter.out.relay.models.OutboxEventJpaEntity
import br.com.souza.saga_orchestrator.application.domain.model.OutboxEvent
import br.com.souza.saga_orchestrator.application.domain.model.OutboxEventStatus

fun OutboxEvent.toJpaEntity() = OutboxEventJpaEntity(
    id = id ?: "",
    aggregateId = aggregateId,
    aggregateType = aggregateType,
    eventType = eventType,
    topic = topic,
    payload = payload,
    traceParent = traceParent,
    status = status.name,
    retriesCount = retriesCount,
    maxRetries = maxRetries,
    createdAt = createdAt,
    sentAt = sentAt,
    lockedAt = lockedAt
)

fun OutboxEventJpaEntity.toDomain() = OutboxEvent(
    id = id,
    aggregateId = aggregateId,
    aggregateType = aggregateType,
    eventType = eventType,
    topic = topic,
    payload = payload,
    traceParent = traceParent,
    status = OutboxEventStatus.valueOf(status),
    retriesCount = retriesCount,
    maxRetries = maxRetries,
    createdAt = createdAt,
    sentAt = sentAt,
    lockedAt = lockedAt
)
```

- [ ] **Step 5: Create repository adapters**

`SagaRepositoryAdapter.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga

import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toDomain
import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toJpaEntity
import br.com.souza.saga_orchestrator.adapter.out.saga.repository.SagaJpaRepository
import br.com.souza.saga_orchestrator.application.domain.model.Saga
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class SagaRepositoryAdapter(
    private val jpaRepository: SagaJpaRepository
) : SagaRepositoryPort {

    override fun save(saga: Saga): Saga =
        jpaRepository.save(saga.toJpaEntity()).toDomain()

    override fun findById(id: String): Saga? =
        jpaRepository.findById(id).orElse(null)?.toDomain()

    override fun findByOrderId(orderId: String): Saga? =
        jpaRepository.findByOrderId(orderId)?.toDomain()

    override fun findTimedOutSagas(olderThan: LocalDateTime): List<Saga> =
        jpaRepository.findTimedOutSagas(olderThan).map { it.toDomain() }
}
```

`SagaHistoryRepositoryAdapter.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga

import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toDomain
import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toJpaEntity
import br.com.souza.saga_orchestrator.adapter.out.saga.repository.SagaHistoryJpaRepository
import br.com.souza.saga_orchestrator.application.domain.model.SagaHistory
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import org.springframework.stereotype.Component

@Component
class SagaHistoryRepositoryAdapter(
    private val jpaRepository: SagaHistoryJpaRepository
) : SagaHistoryRepositoryPort {

    override fun save(history: SagaHistory): SagaHistory =
        jpaRepository.save(history.toJpaEntity()).toDomain()

    override fun findBySagaId(sagaId: String): List<SagaHistory> =
        jpaRepository.findBySagaIdOrderByCreatedAtAsc(sagaId).map { it.toDomain() }
}
```

`OutboxEventRepositoryAdapter.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.out.relay

import br.com.souza.saga_orchestrator.adapter.out.relay.mappers.toDomain
import br.com.souza.saga_orchestrator.adapter.out.relay.mappers.toJpaEntity
import br.com.souza.saga_orchestrator.adapter.out.relay.repository.OutboxEventJpaRepository
import br.com.souza.saga_orchestrator.application.domain.model.OutboxEvent
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class OutboxEventRepositoryAdapter(
    private val jpaRepository: OutboxEventJpaRepository
) : OutboxEventRepositoryPort {

    override fun save(event: OutboxEvent): OutboxEvent =
        jpaRepository.save(event.toJpaEntity()).toDomain()

    override fun findPendingEvents(limit: Int): List<OutboxEvent> {
        val expiredBefore = LocalDateTime.now().minusMinutes(5)
        return jpaRepository.findPendingEvents(limit, expiredBefore).map { it.toDomain() }
    }

    override fun lockEvents(ids: List<String>): Int =
        jpaRepository.lockEvents(ids, LocalDateTime.now())

    override fun markAsSent(id: String) =
        jpaRepository.markAsSent(id, LocalDateTime.now())

    override fun markAsFailed(id: String) =
        jpaRepository.markAsFailed(id)

    override fun markAsDeadLetter(id: String) =
        jpaRepository.markAsDeadLetter(id)
}
```

- [ ] **Step 6: Verify project compiles**

Run: `cd saga-orchestrator && ./mvnw compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 7: Commit**

```bash
git add saga-orchestrator/src/
git commit -m "feat(orchestrator): add persistence layer with JPA entities, ports, and adapters"
```

---

### Task 4: Saga Orchestrator — SagaManager, Outbox Relay, Kafka Config & Reply Consumers

**Files:**
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/ports/in/StartSagaUseCase.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/ports/in/HandleReplyUseCase.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/application/domain/service/SagaManager.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/relay/OutboxRelayScheduler.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/infrastructure/kafka/KafkaConfig.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/infrastructure/observability/TraceContextExtractor.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/in/consumer/dto/SagaReplyEvent.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/in/consumer/dto/OrderCreatedReply.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/in/consumer/OrdersReplyConsumer.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/in/consumer/InventoryReplyConsumer.kt`
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/in/consumer/PaymentsReplyConsumer.kt`
- Create: `saga-orchestrator/src/test/kotlin/br/com/souza/saga_orchestrator/application/domain/service/SagaManagerTest.kt`

**Interfaces:**
- Consumes: `Saga`, `SagaHistory`, `SagaStep`, `ReplyStatus`, `OutboxEvent` from Task 1; `SagaStateMachine`, `Transition` from Task 2; `SagaRepositoryPort`, `SagaHistoryRepositoryPort`, `OutboxEventRepositoryPort` from Task 3
- Produces:
  - `StartSagaUseCase.execute(orderId: String, payload: String, traceParent: String?)`
  - `HandleReplyUseCase.execute(sagaId: String, status: ReplyStatus, reason: String?, traceParent: String?)`

- [ ] **Step 1: Create input ports**

`StartSagaUseCase.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.ports.`in`

interface StartSagaUseCase {
    fun execute(orderId: String, payload: String, traceParent: String?)
}
```

`HandleReplyUseCase.kt`:
```kotlin
package br.com.souza.saga_orchestrator.application.ports.`in`

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus

interface HandleReplyUseCase {
    fun execute(sagaId: String, status: ReplyStatus, reason: String?, traceParent: String?)
}
```

- [ ] **Step 2: Write the failing test for SagaManager**

```kotlin
package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.*
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.kotlin.*
import org.mockito.junit.jupiter.MockitoExtension
import kotlin.test.assertEquals

@ExtendWith(MockitoExtension::class)
class SagaManagerTest {

    private val sagaRepository: SagaRepositoryPort = mock()
    private val sagaHistoryRepository: SagaHistoryRepositoryPort = mock()
    private val outboxRepository: OutboxEventRepositoryPort = mock()
    private val stateMachine = SagaStateMachine()

    private val sagaManager = SagaManager(
        sagaRepository = sagaRepository,
        sagaHistoryRepository = sagaHistoryRepository,
        outboxRepository = outboxRepository,
        stateMachine = stateMachine
    )

    @Test
    fun `startSaga should create saga, record history, and emit first command`() {
        val payload = """{"orderId":"order-1","userId":"user-1","productId":1,"quantity":2,"paymentType":"PIX"}"""

        whenever(sagaRepository.findByOrderId("order-1")).thenReturn(null)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("order-1", payload, "00-traceid-spanid-01")

        // Verify saga saved twice: once as STARTED, once as RESERVING_STOCK
        argumentCaptor<Saga>().apply {
            verify(sagaRepository, times(2)).save(capture())
            assertEquals(SagaStep.STARTED, firstValue.currentStep)
            assertEquals(SagaStep.RESERVING_STOCK, secondValue.currentStep)
        }

        // Verify two history entries: STARTED and RESERVING_STOCK
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository, times(2)).save(capture())
            assertEquals(SagaStep.STARTED, firstValue.step)
            assertEquals("CREATED", firstValue.status)
            assertEquals(SagaStep.RESERVING_STOCK, secondValue.step)
            assertEquals("PENDING", secondValue.status)
        }

        // Verify outbox event for reserve-stock command
        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("inventory.commands.reserve-stock", firstValue.topic)
            assertEquals("RESERVE_STOCK", firstValue.eventType)
            assertEquals("SAGA", firstValue.aggregateType)
        }
    }

    @Test
    fun `startSaga should be idempotent if saga already exists`() {
        val existingSaga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.RESERVING_STOCK,
            payload = "{}"
        )
        whenever(sagaRepository.findByOrderId("order-1")).thenReturn(existingSaga)

        sagaManager.execute("order-1", "{}", null)

        verify(sagaRepository, never()).save(any())
    }

    @Test
    fun `onReply should advance saga and emit next command`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.RESERVING_STOCK,
            payload = """{"orderId":"order-1","productId":1,"quantity":2,"paymentType":"PIX","amount":5000}"""
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("saga-1", ReplyStatus.SUCCESS, null, "00-traceid-spanid-01")

        // Verify saga updated to PROCESSING_PAYMENT
        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.PROCESSING_PAYMENT, firstValue.currentStep)
        }

        // Verify history entry
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository).save(capture())
            assertEquals(SagaStep.PROCESSING_PAYMENT, firstValue.step)
        }

        // Verify outbox event for process-payment command
        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("payments.commands.process-payment", firstValue.topic)
        }
    }

    @Test
    fun `onReply should handle failure and start compensation`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.PROCESSING_PAYMENT,
            payload = """{"orderId":"order-1","productId":1,"quantity":2,"paymentType":"BOLETO"}"""
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("saga-1", ReplyStatus.FAILURE, "BOLETO_NOT_ACCEPTED", null)

        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.RELEASING_STOCK, firstValue.currentStep)
        }

        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("inventory.commands.release-stock", firstValue.topic)
        }
    }

    @Test
    fun `onReply to terminal state should not emit outbox event`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.CONFIRMING_RESERVATION,
            payload = "{}"
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }

        sagaManager.execute("saga-1", ReplyStatus.SUCCESS, null, null)

        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.COMPLETED, firstValue.currentStep)
        }

        verify(outboxRepository, never()).save(any())
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd saga-orchestrator && ./mvnw test -Dtest=SagaManagerTest -q`
Expected: FAIL — `SagaManager` class does not exist

- [ ] **Step 4: Implement SagaManager**

```kotlin
package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.*
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.application.ports.`in`.StartSagaUseCase
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.util.UUID

@Service
class SagaManager(
    private val sagaRepository: SagaRepositoryPort,
    private val sagaHistoryRepository: SagaHistoryRepositoryPort,
    private val outboxRepository: OutboxEventRepositoryPort,
    private val stateMachine: SagaStateMachine
) : StartSagaUseCase, HandleReplyUseCase {

    private val logger = LoggerFactory.getLogger(SagaManager::class.java)

    @Transactional
    override fun execute(orderId: String, payload: String, traceParent: String?) {
        // Idempotency: skip if saga already exists for this order
        if (sagaRepository.findByOrderId(orderId) != null) {
            logger.info("Saga already exists for order, skipping", kv("order_id", orderId))
            return
        }

        val sagaId = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        // Create saga in STARTED state
        val saga = Saga(
            id = sagaId,
            orderId = orderId,
            currentStep = SagaStep.STARTED,
            payload = payload,
            createdAt = now,
            updatedAt = now
        )
        sagaRepository.save(saga)
        logger.info("Saga created", kv("saga_id", sagaId), kv("order_id", orderId))

        // Record initial history
        sagaHistoryRepository.save(
            SagaHistory(
                id = UUID.randomUUID().toString(),
                sagaId = sagaId,
                step = SagaStep.STARTED,
                status = "CREATED",
                createdAt = now
            )
        )

        // Transition to first step
        val transition = stateMachine.transition(SagaStep.STARTED, ReplyStatus.CREATED)
        advanceSaga(saga, transition, traceParent)
    }

    @Transactional
    override fun execute(sagaId: String, status: ReplyStatus, reason: String?, traceParent: String?) {
        val saga = sagaRepository.findById(sagaId)
        if (saga == null) {
            logger.warn("Saga not found", kv("saga_id", sagaId))
            return
        }

        if (saga.currentStep == SagaStep.COMPLETED || saga.currentStep == SagaStep.FAILED) {
            logger.info("Saga already in terminal state, skipping", kv("saga_id", sagaId), kv("step", saga.currentStep.name))
            return
        }

        logger.info("Processing reply", kv("saga_id", sagaId), kv("current_step", saga.currentStep.name), kv("status", status.name))

        val transition = stateMachine.transition(saga.currentStep, status)
        advanceSaga(saga, transition, traceParent, reason)
    }

    private fun advanceSaga(saga: Saga, transition: Transition, traceParent: String?, reason: String? = null) {
        val now = LocalDateTime.now()

        // Update saga state
        val updatedSaga = saga.copy(
            currentStep = transition.nextStep,
            updatedAt = now
        )
        sagaRepository.save(updatedSaga)

        // Record history
        sagaHistoryRepository.save(
            SagaHistory(
                id = UUID.randomUUID().toString(),
                sagaId = saga.id,
                step = transition.nextStep,
                status = if (transition.commandTopic != null) "PENDING" else transition.nextStep.name,
                reason = reason,
                createdAt = now
            )
        )

        logger.info("Saga advanced", kv("saga_id", saga.id), kv("new_step", transition.nextStep.name))

        // Emit command if not terminal
        if (transition.commandTopic != null && transition.commandEventType != null) {
            val outboxEvent = OutboxEvent(
                id = UUID.randomUUID().toString(),
                aggregateId = saga.id,
                aggregateType = "SAGA",
                eventType = transition.commandEventType,
                topic = transition.commandTopic,
                payload = saga.payload,
                traceParent = traceParent
            )
            outboxRepository.save(outboxEvent)
            logger.info("Command queued", kv("saga_id", saga.id), kv("topic", transition.commandTopic))
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd saga-orchestrator && ./mvnw test -Dtest=SagaManagerTest -q`
Expected: All 5 tests PASS

- [ ] **Step 6: Create TraceContextExtractor**

```kotlin
package br.com.souza.saga_orchestrator.infrastructure.observability

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import org.springframework.stereotype.Component

@Component
class TraceContextExtractor {

    private val getter = object : TextMapGetter<Map<String, String>> {
        override fun keys(carrier: Map<String, String>): Iterable<String> = carrier.keys
        override fun get(carrier: Map<String, String>?, key: String): String? = carrier?.get(key)
    }

    fun extractContext(traceParent: String?): Context {
        if (traceParent == null) return Context.current()
        val carrier = mapOf("traceparent" to traceParent)
        return GlobalOpenTelemetry.getPropagators()
            .textMapPropagator
            .extract(Context.current(), carrier, getter)
    }
}
```

- [ ] **Step 7: Create KafkaConfig**

```kotlin
package br.com.souza.saga_orchestrator.infrastructure.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) {

    @Bean
    fun ordersReplyConsumerFactory(): ConsumerFactory<String, Any> {
        val props = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "saga-orchestrator",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            JsonDeserializer.TRUSTED_PACKAGES to "*",
            JsonDeserializer.USE_TYPE_INFO_HEADERS to false,
            JsonDeserializer.VALUE_DEFAULT_TYPE to "br.com.souza.saga_orchestrator.adapter.in.consumer.dto.OrderCreatedReply"
        )
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun ordersReplyKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = ordersReplyConsumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
        return factory
    }

    @Bean
    fun sagaReplyConsumerFactory(): ConsumerFactory<String, Any> {
        val props = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "saga-orchestrator",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            JsonDeserializer.TRUSTED_PACKAGES to "*",
            JsonDeserializer.USE_TYPE_INFO_HEADERS to false,
            JsonDeserializer.VALUE_DEFAULT_TYPE to "br.com.souza.saga_orchestrator.adapter.in.consumer.dto.SagaReplyEvent"
        )
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun sagaReplyKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = sagaReplyConsumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
        return factory
    }

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        )
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }
}
```

- [ ] **Step 8: Create DTOs and Reply Consumers**

`OrderCreatedReply.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class OrderCreatedReply(
    val orderId: String = "",
    val userId: String = "",
    val productId: Int = 0,
    val quantity: Int = 0,
    val paymentType: String = "",
    val status: String = ""
)
```

`SagaReplyEvent.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class SagaReplyEvent(
    val sagaId: String = "",
    val orderId: String = "",
    val status: String = "",
    val reason: String? = null
)
```

`OrdersReplyConsumer.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.`in`.consumer

import br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto.OrderCreatedReply
import br.com.souza.saga_orchestrator.application.ports.`in`.StartSagaUseCase
import br.com.souza.saga_orchestrator.infrastructure.observability.TraceContextExtractor
import com.fasterxml.jackson.databind.ObjectMapper
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class OrdersReplyConsumer(
    private val startSaga: StartSagaUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(OrdersReplyConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("saga-orchestrator")
    private val objectMapper = ObjectMapper()

    @KafkaListener(
        topics = ["orders.replies"],
        groupId = "saga-orchestrator",
        containerFactory = "ordersReplyKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload reply: OrderCreatedReply,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("orders.replies process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received order created reply", kv("order_id", reply.orderId), kv("status", reply.status))

                if (reply.status != "CREATED") {
                    logger.info("Ignoring non-CREATED reply", kv("order_id", reply.orderId), kv("status", reply.status))
                    return
                }

                val payload = objectMapper.writeValueAsString(reply)
                startSaga.execute(reply.orderId, payload, traceParent)
            } finally {
                span.end()
            }
        }
    }
}
```

`InventoryReplyConsumer.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.`in`.consumer

import br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto.SagaReplyEvent
import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class InventoryReplyConsumer(
    private val handleReply: HandleReplyUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(InventoryReplyConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("saga-orchestrator")

    @KafkaListener(
        topics = ["inventory.replies"],
        groupId = "saga-orchestrator",
        containerFactory = "sagaReplyKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload reply: SagaReplyEvent,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.replies process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received inventory reply", kv("saga_id", reply.sagaId), kv("status", reply.status))
                handleReply.execute(reply.sagaId, ReplyStatus.valueOf(reply.status), reply.reason, traceParent)
            } finally {
                span.end()
            }
        }
    }
}
```

`PaymentsReplyConsumer.kt`:
```kotlin
package br.com.souza.saga_orchestrator.adapter.`in`.consumer

import br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto.SagaReplyEvent
import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class PaymentsReplyConsumer(
    private val handleReply: HandleReplyUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(PaymentsReplyConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("saga-orchestrator")

    @KafkaListener(
        topics = ["payments.replies"],
        groupId = "saga-orchestrator",
        containerFactory = "sagaReplyKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload reply: SagaReplyEvent,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("payments.replies process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received payments reply", kv("saga_id", reply.sagaId), kv("status", reply.status))
                handleReply.execute(reply.sagaId, ReplyStatus.valueOf(reply.status), reply.reason, traceParent)
            } finally {
                span.end()
            }
        }
    }
}
```

- [ ] **Step 9: Create OutboxRelayScheduler**

```kotlin
package br.com.souza.saga_orchestrator.adapter.out.relay

import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.nio.charset.StandardCharsets

@Component
class OutboxRelayScheduler(
    private val outboxRepository: OutboxEventRepositoryPort,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(OutboxRelayScheduler::class.java)

    @Scheduled(fixedDelay = 1000)
    @Transactional
    fun relay() {
        val events = outboxRepository.findPendingEvents(50)
        if (events.isEmpty()) return

        val ids = events.mapNotNull { it.id }
        outboxRepository.lockEvents(ids)

        for (event in events) {
            val id = event.id ?: continue
            try {
                val key = "${event.aggregateType}:${event.aggregateId}"
                val record = ProducerRecord<String, String>(event.topic, key, event.payload)
                if (event.traceParent != null) {
                    record.headers().add("traceparent", event.traceParent.toByteArray(StandardCharsets.UTF_8))
                }

                kafkaTemplate.send(record).get()
                outboxRepository.markAsSent(id)
                logger.info("Outbox event published", kv("id", id), kv("topic", event.topic))
            } catch (ex: Exception) {
                logger.error("Failed to publish outbox event", kv("id", id), ex)
                if (event.retriesCount + 1 >= event.maxRetries) {
                    outboxRepository.markAsDeadLetter(id)
                    logger.warn("Outbox event moved to dead letter", kv("id", id))
                } else {
                    outboxRepository.markAsFailed(id)
                }
            }
        }
    }
}
```

- [ ] **Step 10: Run all tests**

Run: `cd saga-orchestrator && ./mvnw test -q`
Expected: All tests PASS

- [ ] **Step 11: Commit**

```bash
git add saga-orchestrator/src/
git commit -m "feat(orchestrator): implement SagaManager, reply consumers, outbox relay, and Kafka config"
```

---

### Task 5: Saga Orchestrator — Timeout Handler

**Files:**
- Create: `saga-orchestrator/src/main/kotlin/br/com/souza/saga_orchestrator/adapter/out/saga/SagaTimeoutScheduler.kt`

**Interfaces:**
- Consumes: `SagaRepositoryPort` from Task 3, `HandleReplyUseCase` from Task 4
- Produces: `SagaTimeoutScheduler` — scheduled job that compensates timed-out sagas

- [ ] **Step 1: Implement the timeout scheduler**

```kotlin
package br.com.souza.saga_orchestrator.adapter.out.saga

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class SagaTimeoutScheduler(
    private val sagaRepository: SagaRepositoryPort,
    private val handleReply: HandleReplyUseCase,
    @Value("\${saga.timeout-minutes:5}") private val timeoutMinutes: Long
) {

    private val logger = LoggerFactory.getLogger(SagaTimeoutScheduler::class.java)

    @Scheduled(fixedDelay = 60000)
    fun checkTimeouts() {
        val cutoff = LocalDateTime.now().minusMinutes(timeoutMinutes)
        val timedOut = sagaRepository.findTimedOutSagas(cutoff)

        for (saga in timedOut) {
            logger.warn("Saga timed out, starting compensation",
                kv("saga_id", saga.id),
                kv("order_id", saga.orderId),
                kv("current_step", saga.currentStep.name)
            )
            try {
                handleReply.execute(saga.id, ReplyStatus.FAILURE, "TIMEOUT", null)
            } catch (ex: Exception) {
                logger.error("Error compensating timed out saga", kv("saga_id", saga.id), ex)
            }
        }
    }
}
```

- [ ] **Step 2: Verify project compiles**

Run: `cd saga-orchestrator && ./mvnw compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add saga-orchestrator/src/
git commit -m "feat(orchestrator): add timeout handler for stale sagas"
```

---

### Task 6: Orders Service — Migrate to Command Handler

**Files:**
- Modify: `orders-service/internal/service/order.go` — change `CreateOrder` to publish to `orders.replies` instead of `orders.created`; change `ConfirmOrder` to not emit outbox event; add reply publishing to `ConfirmOrder` and `CancelOrder`
- Modify: `orders-service/internal/domain/order.go` — add `OrderCreatedReply` struct, add `SagaCommand` struct
- Create: `orders-service/internal/domain/saga.go` — saga command DTOs
- Create: `orders-service/internal/consumer/confirm_order.go` — consumer for `orders.commands.confirm-order`
- Create: `orders-service/internal/consumer/cancel_order.go` — consumer for `orders.commands.cancel-order`
- Delete: `orders-service/internal/consumer/payments.go`
- Delete: `orders-service/internal/consumer/inventory_insufficient_stock.go`
- Delete: `orders-service/internal/consumer/inventory_released.go`
- Modify: `orders-service/internal/config/properties.go` — replace topic configs
- Modify: `orders-service/config.yaml` — replace topic names
- Modify: `orders-service/cmd/api/main.go` — rewire consumers and producers

**Interfaces:**
- Consumes: Kafka commands from orchestrator: `{sagaId, orderId}` on `orders.commands.confirm-order`, `{sagaId, orderId, reason}` on `orders.commands.cancel-order`
- Produces: Kafka replies on `orders.replies`: `{sagaId, orderId, status: SUCCESS/FAILURE}` for confirm/cancel commands; `{orderId, userId, productId, quantity, paymentType, status: CREATED}` for order creation trigger

- [ ] **Step 1: Create `orders-service/internal/domain/saga.go`**

```go
package domain

type ConfirmOrderCommand struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
}

type CancelOrderCommand struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

type SagaReply struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}

type OrderCreatedReply struct {
	OrderID     string          `json:"orderId"`
	UserID      string          `json:"userId"`
	ProductID   int             `json:"productId"`
	Quantity    int             `json:"quantity"`
	PaymentType PaymentTypeEnum `json:"paymentType"`
	Status      string          `json:"status"`
}
```

- [ ] **Step 2: Update `orders-service/internal/config/properties.go`**

Replace `KafkaConfig` with:

```go
type KafkaConfig struct {
	Brokers             []string `mapstructure:"brokers"`
	OrdersRepliesTopic  string   `mapstructure:"orders_replies_topic"`
	ConfirmOrderTopic   string   `mapstructure:"confirm_order_topic"`
	CancelOrderTopic    string   `mapstructure:"cancel_order_topic"`
}
```

Update `Load()` env bindings to:

```go
v.BindEnv("kafka.orders_replies_topic", "KAFKA_ORDERS_REPLIES_TOPIC")
v.BindEnv("kafka.confirm_order_topic", "KAFKA_CONFIRM_ORDER_TOPIC")
v.BindEnv("kafka.cancel_order_topic", "KAFKA_CANCEL_ORDER_TOPIC")
```

Remove the old bindings for `orders_created_topic`, `orders_confirmed_topic`, `inventory_topic`, `inventory_released_topic`, `payments_topic`.

- [ ] **Step 3: Update `orders-service/config.yaml`**

```yaml
server:
  port: ":8081"

mysql:
  dsn: "root:root@tcp(localhost:3307)/orders?parseTime=true"

redis:
  addr: "localhost:6379"
  pass: ""
  db: 0

kafka:
  brokers:
    - "localhost:29092"
  orders_replies_topic: "orders.replies"
  confirm_order_topic: "orders.commands.confirm-order"
  cancel_order_topic: "orders.commands.cancel-order"

outbox:
  batch_size: 10

otel:
  exporter_endpoint: "localhost:4317"
```

- [ ] **Step 4: Update `orders-service/internal/service/order.go`**

Change `CreateOrder` to publish `OrderCreatedReply` to `orders.replies` instead of `orders.created`:

```go
func (os *OrderService) CreateOrder(ctx context.Context, request domain.OrderRequest) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	orderId, err := os.orderRepository.Save(ctx, tx, request)
	if err != nil {
		return fmt.Errorf("creating order: %w", err)
	}

	reply := domain.OrderCreatedReply{
		OrderID:     orderId,
		UserID:      request.UserID,
		ProductID:   request.ProductID,
		Quantity:    request.Quantity,
		PaymentType: request.PaymentType,
		Status:      "CREATED",
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling order created reply: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.replies",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("creating order: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.With(zap.String("order_id", orderId)).Info("Order created")
	return nil
}
```

Simplify `ConfirmOrder` — no longer publishes outbox event, just updates status and publishes reply:

```go
func (os *OrderService) ConfirmOrder(ctx context.Context, sagaId string, orderId string) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	updated, err := os.orderRepository.ConfirmOrder(ctx, tx, orderId)
	if err != nil {
		return fmt.Errorf("confirming order: %w", err)
	}

	if !updated {
		log.Info("order already processed, skipping", zap.String("order_id", orderId))
		return nil
	}

	reply := domain.SagaReply{
		SagaID:  sagaId,
		OrderID: orderId,
		Status:  "SUCCESS",
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling reply: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.replies",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("saving outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Info("order confirmed", zap.String("order_id", orderId))
	return nil
}
```

Update `CancelOrder` to publish reply:

```go
func (os *OrderService) CancelOrder(ctx context.Context, sagaId string, orderId string, reason string) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	if err := os.orderRepository.UpdateStatusToCanceled(ctx, tx, orderId, reason); err != nil {
		return fmt.Errorf("canceling order: %w", err)
	}

	reply := domain.SagaReply{
		SagaID:  sagaId,
		OrderID: orderId,
		Status:  "SUCCESS",
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling reply: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.replies",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("saving outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Info("order canceled", zap.String("order_id", orderId), zap.String("reason", reason))
	return nil
}
```

Note: `UpdateStatusToCanceled` needs to accept a `DBTX` instead of `*sql.DB` so it can participate in the transaction. Adjust the repository interface and implementation accordingly.

- [ ] **Step 5: Create command consumers**

`orders-service/internal/consumer/confirm_order.go`:
```go
package consumer

import (
	"context"
	"encoding/json"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type ConfirmOrderConsumer struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	orderService *service.OrderService
}

func NewConfirmOrderConsumer(logger *zap.Logger, reader *kafka.Reader, orderService *service.OrderService) *ConfirmOrderConsumer {
	return &ConfirmOrderConsumer{
		logger:       logger,
		reader:       reader,
		orderService: orderService,
	}
}

func (c *ConfirmOrderConsumer) Start(ctx context.Context) {
	c.logger.Info("confirm-order consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("confirm-order consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *ConfirmOrderConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("confirm-order-consumer")
	ctx, span := tracer.Start(ctx, "orders.commands.confirm-order process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	var cmd domain.ConfirmOrderCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Error("error unmarshalling message", zap.Error(err))
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing confirm-order command",
		zap.String("saga_id", cmd.SagaID),
		zap.String("order_id", cmd.OrderID),
	)

	if err := c.orderService.ConfirmOrder(ctx, cmd.SagaID, cmd.OrderID); err != nil {
		log.Error("error confirming order", zap.String("order_id", cmd.OrderID), zap.Error(err))
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *ConfirmOrderConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}
```

`orders-service/internal/consumer/cancel_order.go`:
```go
package consumer

import (
	"context"
	"encoding/json"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type CancelOrderConsumer struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	orderService *service.OrderService
}

func NewCancelOrderConsumer(logger *zap.Logger, reader *kafka.Reader, orderService *service.OrderService) *CancelOrderConsumer {
	return &CancelOrderConsumer{
		logger:       logger,
		reader:       reader,
		orderService: orderService,
	}
}

func (c *CancelOrderConsumer) Start(ctx context.Context) {
	c.logger.Info("cancel-order consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("cancel-order consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *CancelOrderConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("cancel-order-consumer")
	ctx, span := tracer.Start(ctx, "orders.commands.cancel-order process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	var cmd domain.CancelOrderCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Error("error unmarshalling message", zap.Error(err))
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing cancel-order command",
		zap.String("saga_id", cmd.SagaID),
		zap.String("order_id", cmd.OrderID),
		zap.String("reason", cmd.Reason),
	)

	if err := c.orderService.CancelOrder(ctx, cmd.SagaID, cmd.OrderID, cmd.Reason); err != nil {
		log.Error("error canceling order", zap.String("order_id", cmd.OrderID), zap.Error(err))
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *CancelOrderConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}
```

- [ ] **Step 6: Delete old consumers**

Delete:
- `orders-service/internal/consumer/payments.go`
- `orders-service/internal/consumer/inventory_insufficient_stock.go`
- `orders-service/internal/consumer/inventory_released.go`

Keep `trace_context.go` — it's still used by the new consumers.

- [ ] **Step 7: Update `orders-service/cmd/api/main.go`**

Replace the producers section:
```go
// kafka producer — single topic: orders.replies
ordersRepliesWriter := config.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.OrdersRepliesTopic)
defer ordersRepliesWriter.Close()

writers := map[string]*kafka.Writer{
    cfg.Kafka.OrdersRepliesTopic: ordersRepliesWriter,
}
```

Replace the consumers section:
```go
// kafka consumers — command handlers
confirmOrderReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.ConfirmOrderTopic, "orders-service-group")
defer confirmOrderReader.Close()

cancelOrderReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.CancelOrderTopic, "orders-service-group")
defer cancelOrderReader.Close()

confirmOrderConsumer := consumer.NewConfirmOrderConsumer(logger, confirmOrderReader, orderService)
go func() {
    defer func() {
        if r := recover(); r != nil {
            logger.Error("confirm-order consumer panic", zap.Any("panic", r))
        }
    }()
    confirmOrderConsumer.Start(relayCtx)
}()

cancelOrderConsumer := consumer.NewCancelOrderConsumer(logger, cancelOrderReader, orderService)
go func() {
    defer func() {
        if r := recover(); r != nil {
            logger.Error("cancel-order consumer panic", zap.Any("panic", r))
        }
    }()
    cancelOrderConsumer.Start(relayCtx)
}()
```

Remove all old consumer instantiations (inventoryConsumer, inventoryReleasedConsumer, paymentsConsumer) and their kafka readers.

- [ ] **Step 8: Verify project compiles**

Run: `cd orders-service && go build ./...`
Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```bash
git add orders-service/
git commit -m "feat(orders): migrate from choreography to command handler with saga replies"
```

---

### Task 7: Payments Service — Migrate to Command Handler

**Files:**
- Modify: `payments-service/internal/service/payment.go` — accept `SagaCommand` instead of `InventoryReservedEvent`, publish reply to `payments.replies`
- Modify: `payments-service/internal/domain/payment.go` — add `ProcessPaymentCommand`, `SagaReply` structs
- Create: `payments-service/internal/consumer/process_payment.go` — consumer for `payments.commands.process-payment`
- Delete: `payments-service/internal/consumer/inventory.go`
- Modify: `payments-service/internal/config/properties.go` — replace topic configs
- Modify: `payments-service/config.yaml` — replace topic names
- Modify: `payments-service/cmd/api/main.go` — rewire consumers and producers

**Interfaces:**
- Consumes: Kafka command `{sagaId, orderId, amount, paymentType}` on `payments.commands.process-payment`
- Produces: Kafka reply on `payments.replies`: `{sagaId, orderId, status: SUCCESS/FAILURE, reason?}`

- [ ] **Step 1: Add domain types to `payments-service/internal/domain/payment.go`**

Add to the existing file:

```go
type ProcessPaymentCommand struct {
	SagaID      string `json:"sagaId"`
	OrderID     string `json:"orderId"`
	Amount      int    `json:"amount"`
	PaymentType string `json:"paymentType"`
}

type SagaReply struct {
	SagaID  string `json:"sagaId"`
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}
```

- [ ] **Step 2: Update `payments-service/internal/config/properties.go`**

Replace `KafkaConfig`:
```go
type KafkaConfig struct {
	Brokers              []string `mapstructure:"brokers"`
	ProcessPaymentTopic  string   `mapstructure:"process_payment_topic"`
	PaymentsRepliesTopic string   `mapstructure:"payments_replies_topic"`
}
```

Update env bindings:
```go
v.BindEnv("kafka.process_payment_topic", "KAFKA_PROCESS_PAYMENT_TOPIC")
v.BindEnv("kafka.payments_replies_topic", "KAFKA_PAYMENTS_REPLIES_TOPIC")
```

Remove old bindings for `inventory_reserved_topic`, `payment_authorized_topic`, `payment_denied_topic`.

- [ ] **Step 3: Update `payments-service/config.yaml`**

```yaml
server:
  port: ":8083"

mysql:
  dsn: "root:root@tcp(localhost:3308)/payments?parseTime=true"

kafka:
  brokers:
    - "localhost:29092"
  process_payment_topic: "payments.commands.process-payment"
  payments_replies_topic: "payments.replies"

outbox:
  batch_size: 10

otel:
  exporter_endpoint: "localhost:4317"
```

- [ ] **Step 4: Update `payments-service/internal/service/payment.go`**

Change `ProcessPayment` to accept `ProcessPaymentCommand` and always publish to `payments.replies`:

```go
func (s *PaymentService) ProcessPayment(ctx context.Context, cmd domain.ProcessPaymentCommand, traceParent string) error {
	log := logger.FromContext(ctx, s.logger)

	exists, err := s.paymentRepo.ExistsByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return fmt.Errorf("checking idempotency: %w", err)
	}
	if exists {
		log.Info("payment already processed, skipping", zap.String("order_id", cmd.OrderID))
		return nil
	}

	event := domain.InventoryReservedEvent{
		OrderID:     cmd.OrderID,
		Amount:      cmd.Amount,
		PaymentType: cmd.PaymentType,
	}
	status, reason := s.evaluate(event)

	paymentID := uuid.New().String()
	now := time.Now()

	payment := domain.Payment{
		ID:          paymentID,
		OrderID:     cmd.OrderID,
		Amount:      cmd.Amount,
		PaymentType: domain.PaymentType(cmd.PaymentType),
		Status:      status,
		Reason:      reason,
		CreatedAt:   now,
	}

	replyStatus := "SUCCESS"
	if status == domain.PaymentStatusDenied {
		replyStatus = "FAILURE"
	}

	reply := domain.SagaReply{
		SagaID:  cmd.SagaID,
		OrderID: cmd.OrderID,
		Status:  replyStatus,
		Reason:  reason,
	}

	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshalling reply: %w", err)
	}

	outboxEvent := domain.OutboxEvent{
		AggregateType: "PAYMENT",
		AggregateID:   paymentID,
		EventType:     "payments.replies",
		Payload:       payload,
		TraceParent:   traceParent,
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    5,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	if err := s.paymentRepo.Save(ctx, tx, payment); err != nil {
		return fmt.Errorf("saving payment: %w", err)
	}

	if err := s.outboxRepo.Save(ctx, tx, outboxEvent); err != nil {
		return fmt.Errorf("saving outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Info("payment processed",
		zap.String("payment_id", paymentID),
		zap.String("order_id", cmd.OrderID),
		zap.String("status", string(status)),
	)

	return nil
}
```

- [ ] **Step 5: Create `payments-service/internal/consumer/process_payment.go`**

```go
package consumer

import (
	"context"
	"encoding/json"
	"payments-service/internal/domain"
	"payments-service/internal/logger"
	"payments-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

type ProcessPaymentConsumer struct {
	logger         *zap.Logger
	reader         *kafka.Reader
	paymentService *service.PaymentService
}

func NewProcessPaymentConsumer(logger *zap.Logger, reader *kafka.Reader, paymentService *service.PaymentService) *ProcessPaymentConsumer {
	return &ProcessPaymentConsumer{
		logger:         logger,
		reader:         reader,
		paymentService: paymentService,
	}
}

func (c *ProcessPaymentConsumer) Start(ctx context.Context) {
	c.logger.Info("process-payment consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("process-payment consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *ProcessPaymentConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	traceParent := extractTraceParent(msg.Headers)
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("process-payment-consumer")
	ctx, span := tracer.Start(ctx, "payments.commands.process-payment process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	var cmd domain.ProcessPaymentCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Error("error unmarshalling message", zap.Error(err))
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing process-payment command",
		zap.String("saga_id", cmd.SagaID),
		zap.String("order_id", cmd.OrderID),
		zap.Int("amount", cmd.Amount),
	)

	if err := c.paymentService.ProcessPayment(ctx, cmd, traceParent); err != nil {
		log.Error("error processing payment", zap.String("order_id", cmd.OrderID), zap.Error(err))
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *ProcessPaymentConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}

func extractTraceParent(headers []kafka.Header) string {
	for _, h := range headers {
		if h.Key == "traceparent" {
			return string(h.Value)
		}
	}
	return ""
}

func extractTraceContext(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := make(propagation.MapCarrier)
	for _, h := range headers {
		carrier.Set(h.Key, string(h.Value))
	}
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}
```

- [ ] **Step 6: Delete old consumer**

Delete `payments-service/internal/consumer/inventory.go`

- [ ] **Step 7: Update `payments-service/cmd/api/main.go`**

Replace consumer section:
```go
// kafka consumer — command handler
processPaymentReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.ProcessPaymentTopic, "payments-service-group")
defer processPaymentReader.Close()

processPaymentConsumer := consumer.NewProcessPaymentConsumer(logger, processPaymentReader, paymentService)
go func() {
    defer func() {
        if r := recover(); r != nil {
            logger.Error("process-payment consumer panic", zap.Any("panic", r))
        }
    }()
    processPaymentConsumer.Start(appCtx)
}()
```

Replace producer section:
```go
// kafka producer — single topic: payments.replies
repliesWriter := config.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.PaymentsRepliesTopic)
defer repliesWriter.Close()

writers := map[string]*kafka.Writer{
    "payments.replies": repliesWriter,
}
```

- [ ] **Step 8: Verify project compiles**

Run: `cd payments-service && go build ./...`
Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```bash
git add payments-service/
git commit -m "feat(payments): migrate from choreography to command handler with saga replies"
```

---

### Task 8: Inventory Service — Migrate to Command Handler

**Files:**
- Create: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/saga/ReserveStockCommandConsumer.kt`
- Create: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/saga/ReleaseStockCommandConsumer.kt`
- Create: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/saga/ConfirmReservationCommandConsumer.kt`
- Create: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/saga/dto/SagaCommand.kt`
- Delete: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/order/OrderCreatedKafkaConsumer.kt`
- Delete: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/order/OrderConfirmedKafkaConsumer.kt`
- Delete: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/adapter/in/consumer/payments/PaymentsDeniedKafkaConsumer.kt`
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/application/domain/service/ReserveStockService.kt` — publish reply to `inventory.replies`
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/application/domain/service/ReleaseStockService.kt` — publish reply to `inventory.replies`
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/application/domain/service/ConfirmReservationService.kt` — publish reply to `inventory.replies`
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/application/domain/model/ReserveStockCommand.kt` — add `sagaId` field
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/application/domain/model/ReleaseStockCommand.kt` — add `sagaId` field
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/application/domain/model/ConfirmReservationCommand.kt` — add `sagaId` field
- Modify: `inventory-service/src/main/kotlin/br/com/souza/inventory_service/infrastructure/kafka/KafkaConfig.kt` — replace consumer factories for command topics

**Interfaces:**
- Consumes: Kafka commands `{sagaId, orderId, productId, quantity}` on `inventory.commands.reserve-stock`, `{sagaId, orderId, productId, quantity}` on `inventory.commands.release-stock`, `{sagaId, orderId}` on `inventory.commands.confirm-reservation`
- Produces: Kafka reply on `inventory.replies`: `{sagaId, orderId, status: SUCCESS/FAILURE, reason?}`

- [ ] **Step 1: Create DTO**

`SagaCommand.kt`:
```kotlin
package br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class SagaCommand(
    val sagaId: String = "",
    val orderId: String = "",
    val productId: Int = 0,
    val quantity: Int = 0,
    val paymentType: String = ""
)
```

- [ ] **Step 2: Add `sagaId` to domain command models**

Add `sagaId: String` as first parameter to `ReserveStockCommand`, `ReleaseStockCommand`, and `ConfirmReservationCommand` data classes.

- [ ] **Step 3: Update services to publish replies to `inventory.replies`**

In `ReserveStockService.kt`, change the outbox events:
- Success case: topic = `"inventory.replies"`, eventType = `"INVENTORY_REPLY"`, payload = `{"sagaId": ..., "orderId": ..., "status": "SUCCESS"}`
- Failure case: topic = `"inventory.replies"`, eventType = `"INVENTORY_REPLY"`, payload = `{"sagaId": ..., "orderId": ..., "status": "FAILURE", "reason": ...}`

In `ReleaseStockService.kt`:
- Success: topic = `"inventory.replies"`, payload = `{"sagaId": ..., "orderId": ..., "status": "SUCCESS"}`

In `ConfirmReservationService.kt`:
- Success: topic = `"inventory.replies"`, payload = `{"sagaId": ..., "orderId": ..., "status": "SUCCESS"}`

- [ ] **Step 4: Create command consumers**

`ReserveStockCommandConsumer.kt`:
```kotlin
package br.com.souza.inventory_service.adapter.`in`.consumer.saga

import br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto.SagaCommand
import br.com.souza.inventory_service.application.domain.model.ReserveStockCommand
import br.com.souza.inventory_service.application.ports.`in`.ReserveStockUseCase
import br.com.souza.inventory_service.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ReserveStockCommandConsumer(
    private val reserveStock: ReserveStockUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(ReserveStockCommandConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(
        topics = ["inventory.commands.reserve-stock"],
        groupId = "inventory-service",
        containerFactory = "sagaCommandKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload command: SagaCommand,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.commands.reserve-stock process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received reserve-stock command", kv("saga_id", command.sagaId), kv("order_id", command.orderId))

                val cmd = ReserveStockCommand(
                    sagaId = command.sagaId,
                    orderId = command.orderId,
                    productId = command.productId,
                    quantity = command.quantity,
                    paymentType = command.paymentType,
                    traceParent = traceParent
                )

                reserveStock.execute(cmd)
            } finally {
                span.end()
            }
        }
    }
}
```

`ReleaseStockCommandConsumer.kt`:
```kotlin
package br.com.souza.inventory_service.adapter.`in`.consumer.saga

import br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto.SagaCommand
import br.com.souza.inventory_service.application.domain.model.ReleaseStockCommand
import br.com.souza.inventory_service.application.ports.`in`.ReleaseStockUseCase
import br.com.souza.inventory_service.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ReleaseStockCommandConsumer(
    private val releaseStock: ReleaseStockUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(ReleaseStockCommandConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(
        topics = ["inventory.commands.release-stock"],
        groupId = "inventory-service",
        containerFactory = "sagaCommandKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload command: SagaCommand,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.commands.release-stock process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received release-stock command", kv("saga_id", command.sagaId), kv("order_id", command.orderId))

                val cmd = ReleaseStockCommand(
                    sagaId = command.sagaId,
                    orderId = command.orderId,
                    traceParent = traceParent
                )

                releaseStock.execute(cmd)
            } finally {
                span.end()
            }
        }
    }
}
```

`ConfirmReservationCommandConsumer.kt`:
```kotlin
package br.com.souza.inventory_service.adapter.`in`.consumer.saga

import br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto.SagaCommand
import br.com.souza.inventory_service.application.domain.model.ConfirmReservationCommand
import br.com.souza.inventory_service.application.ports.`in`.ConfirmReservationUseCase
import br.com.souza.inventory_service.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ConfirmReservationCommandConsumer(
    private val confirmReservation: ConfirmReservationUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(ConfirmReservationCommandConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(
        topics = ["inventory.commands.confirm-reservation"],
        groupId = "inventory-service",
        containerFactory = "sagaCommandKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload command: SagaCommand,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.commands.confirm-reservation process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received confirm-reservation command", kv("saga_id", command.sagaId), kv("order_id", command.orderId))

                val cmd = ConfirmReservationCommand(
                    sagaId = command.sagaId,
                    orderId = command.orderId,
                    traceParent = traceParent
                )

                confirmReservation.execute(cmd)
            } finally {
                span.end()
            }
        }
    }
}
```

- [ ] **Step 5: Delete old consumers**

Delete:
- `inventory-service/src/main/kotlin/.../adapter/in/consumer/order/OrderCreatedKafkaConsumer.kt`
- `inventory-service/src/main/kotlin/.../adapter/in/consumer/order/OrderConfirmedKafkaConsumer.kt`
- `inventory-service/src/main/kotlin/.../adapter/in/consumer/payments/PaymentsDeniedKafkaConsumer.kt`

You can also delete the old DTOs if no longer needed:
- `OrderCreatedEvent.kt`, `OrderConfirmedEvent.kt`, `PaymentsDeniedEvent.kt`

- [ ] **Step 6: Update `KafkaConfig.kt`**

Replace the three old consumer factories (orderConsumerFactory, paymentsConsumerFactory, ordersConfirmedConsumerFactory) and their listener container factories with a single factory for saga commands:

```kotlin
@Bean
fun sagaCommandConsumerFactory(): ConsumerFactory<String, Any> {
    val props = mapOf<String, Any>(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to "inventory-service",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to 30000,
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to 10000,
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300000,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
        JsonDeserializer.TRUSTED_PACKAGES to "*",
        JsonDeserializer.USE_TYPE_INFO_HEADERS to false,
        JsonDeserializer.VALUE_DEFAULT_TYPE to "br.com.souza.inventory_service.adapter.in.consumer.saga.dto.SagaCommand"
    )
    return DefaultKafkaConsumerFactory(props)
}

@Bean
fun sagaCommandKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.consumerFactory = sagaCommandConsumerFactory()
    factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
    return factory
}
```

Remove `orderConsumerFactory`, `kafkaListenerContainerFactory`, `paymentsConsumerFactory`, `paymentsKafkaListenerContainerFactory`, `ordersConfirmedConsumerFactory`, `ordersConfirmedKafkaListenerContainerFactory`.

- [ ] **Step 7: Verify project compiles**

Run: `cd inventory-service && ./mvnw compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 8: Commit**

```bash
git add inventory-service/
git commit -m "feat(inventory): migrate from choreography to command handler with saga replies"
```

---

### Task 9: Docker Compose & Kafka Topics Update

**Files:**
- Modify: `docker-compose.yaml` — add orchestrator containers, update kafka-init topics, update service env vars

**Interfaces:**
- Consumes: all previous tasks
- Produces: running infrastructure

- [ ] **Step 1: Add orchestrator containers to `docker-compose.yaml`**

Add after `postgres-inventory`:

```yaml
  postgres-orchestrator:
    image: postgres:17
    container_name: orchestrator-postgres
    environment:
      POSTGRES_DB: saga_db
      POSTGRES_USER: saga
      POSTGRES_PASSWORD: saga
    ports:
      - "5433:5432"
    volumes:
      - postgres_orchestrator_data:/var/lib/postgresql/data
      - ./saga-orchestrator/INIT.sql:/docker-entrypoint-initdb.d/01-saga.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U saga -d saga_db"]
      interval: 5s
      timeout: 3s
      retries: 10
```

Add after `inventory-service`:

```yaml
  saga-orchestrator:
    image: vsouzx/saga-orchestrator:latest
    ports:
      - "8084:8084"
    environment:
      SPRING_DATASOURCE_URL: "jdbc:postgresql://postgres-orchestrator:5432/saga_db"
      SPRING_DATASOURCE_USERNAME: "saga"
      SPRING_DATASOURCE_PASSWORD: "saga"
      SPRING_KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4318"
    depends_on:
      postgres-orchestrator:
        condition: service_healthy
      kafka:
        condition: service_healthy
      otel-collector:
        condition: service_started
```

Add to volumes section:
```yaml
  postgres_orchestrator_data:
```

- [ ] **Step 2: Update kafka-init topics**

Replace the entire command block with new topics (remove old choreography topics, add command/reply topics):

```yaml
  kafka-init:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        echo "Criando topicos Kafka (orchestration)..."

        # Reply topics
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic orders.replies \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic inventory.replies \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic payments.replies \
          --partitions 3 \
          --config retention.ms=604800000

        # Command topics
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic inventory.commands.reserve-stock \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic inventory.commands.release-stock \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic inventory.commands.confirm-reservation \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic payments.commands.process-payment \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic orders.commands.confirm-order \
          --partitions 3 \
          --config retention.ms=604800000

        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic orders.commands.cancel-order \
          --partitions 3 \
          --config retention.ms=604800000

        echo "Topicos criados:"
        kafka-topics --bootstrap-server kafka:9092 --list
```

- [ ] **Step 3: Update orders-service env vars in docker-compose**

```yaml
  orders-service:
    image: vsouzx/orders-service:latest
    ports:
      - "8081:8081"
    environment:
      MYSQL_DSN: "root:root@tcp(mysql-orders:3306)/orders?parseTime=true"
      REDIS_ADDR: "redis:6379"
      KAFKA_BROKERS: "kafka:9092"
      KAFKA_ORDERS_REPLIES_TOPIC: "orders.replies"
      KAFKA_CONFIRM_ORDER_TOPIC: "orders.commands.confirm-order"
      KAFKA_CANCEL_ORDER_TOPIC: "orders.commands.cancel-order"
      OTEL_EXPORTER_ENDPOINT: "otel-collector:4317"
    depends_on:
      mysql-orders:
        condition: service_healthy
      kafka:
        condition: service_healthy
      redis:
        condition: service_started
      otel-collector:
        condition: service_started
```

- [ ] **Step 4: Update payments-service env vars in docker-compose**

```yaml
  payments-service:
    image: vsouzx/payments-service:latest
    ports:
      - "8083:8083"
    environment:
      MYSQL_DSN: "root:root@tcp(mysql-payments:3306)/payments?parseTime=true"
      KAFKA_BROKERS: "kafka:9092"
      KAFKA_PROCESS_PAYMENT_TOPIC: "payments.commands.process-payment"
      KAFKA_PAYMENTS_REPLIES_TOPIC: "payments.replies"
      OTEL_EXPORTER_ENDPOINT: "otel-collector:4317"
    depends_on:
      mysql-payments:
        condition: service_healthy
      kafka:
        condition: service_healthy
      otel-collector:
        condition: service_started
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yaml
git commit -m "feat(infra): add orchestrator to docker-compose, update Kafka topics to command/reply"
```

---

### Task 10: End-to-End Smoke Test

**Files:** none (manual verification)

**Interfaces:**
- Consumes: all running services
- Produces: verified working system

- [ ] **Step 1: Start infrastructure**

Run: `docker compose up -d mysql-orders mysql-payments postgres-inventory postgres-orchestrator redis zookeeper kafka`
Wait for all containers to be healthy.

- [ ] **Step 2: Start kafka-init**

Run: `docker compose up kafka-init`
Expected: All 9 topics created successfully.

- [ ] **Step 3: Start all services locally**

In separate terminals:
- `cd orders-service && go run ./cmd/api`
- `cd payments-service && go run ./cmd/api`
- `cd inventory-service && ./mvnw spring-boot:run`
- `cd saga-orchestrator && ./mvnw spring-boot:run`

- [ ] **Step 4: Test happy path**

```bash
curl -X POST http://localhost:8081/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-happy-1" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","productId":1,"quantity":1,"paymentType":"PIX"}'
```

Expected: 202 Accepted. Check orchestrator logs for saga progression:
`STARTED → RESERVING_STOCK → PROCESSING_PAYMENT → CONFIRMING_ORDER → CONFIRMING_RESERVATION → COMPLETED`

Verify in orchestrator DB:
```sql
-- Connect to saga_db on port 5433
SELECT * FROM sagas WHERE order_id = '<order_id>';
SELECT * FROM saga_history WHERE saga_id = '<saga_id>' ORDER BY created_at;
```

Expected: `current_step = COMPLETED`, and `saga_history` shows 6 rows (STARTED, RESERVING_STOCK, PROCESSING_PAYMENT, CONFIRMING_ORDER, CONFIRMING_RESERVATION, COMPLETED).

- [ ] **Step 5: Test compensation path (payment denied)**

```bash
curl -X POST http://localhost:8081/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-fail-1" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","productId":1,"quantity":1,"paymentType":"BOLETO"}'
```

Expected: Saga follows compensation path:
`STARTED → RESERVING_STOCK → PROCESSING_PAYMENT → RELEASING_STOCK → CANCELING_ORDER → FAILED`

Verify `saga_history` shows 6 rows with the compensation flow.

- [ ] **Step 6: Verify order states**

```bash
curl http://localhost:8081/v1/orders
```

Expected: First order = `CONFIRMED`, second order = `CANCELED` with reason.

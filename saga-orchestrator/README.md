# Saga Orchestrator

Orquestrador central que coordena toda a saga de pedidos. Implementa uma **maquina de estados** que define as transicoes entre steps e emite comandos para os servicos participantes. Consome replies de todos os servicos e avanca ou compensa a saga conforme o resultado. Utiliza **Arquitetura Hexagonal** e **Transactional Outbox**.

**Porta:** 8084 | **Banco:** PostgreSQL | **Linguagem:** Kotlin / Spring Boot 3.4

## Arquitetura (Hexagonal)

```
src/main/kotlin/br/com/souza/saga_orchestrator/
├── adapter/
│   ├── in/
│   │   └── consumer/            # Kafka consumers (replies)
│   │       ├── OrdersReplyConsumer
│   │       ├── InventoryReplyConsumer
│   │       └── PaymentsReplyConsumer
│   └── out/
│       ├── relay/               # OutboxRelayScheduler (polling 1s, batch 50)
│       └── saga/                # Saga & SagaHistory persistence (JPA)
│           └── SagaTimeoutScheduler
├── application/
│   ├── domain/
│   │   ├── model/               # Saga, SagaStep, ReplyStatus, OutboxEvent
│   │   └── service/
│   │       ├── SagaManager      # Logica de orquestracao (StartSaga, HandleReply)
│   │       └── SagaStateMachine # Tabela de transicoes (step, status) -> (nextStep, command)
│   └── ports/
│       ├── in/                  # StartSagaUseCase, HandleReplyUseCase
│       └── out/                 # SagaRepositoryPort, SagaHistoryRepositoryPort, OutboxEventRepositoryPort
└── infrastructure/              # Kafka config, OpenTelemetry
```

## Responsabilidades

- Iniciar saga ao receber reply `orders.replies` (pedido criado)
- Manter estado da saga em banco de dados (PostgreSQL)
- Transicionar entre steps da maquina de estados
- Emitir comandos via Outbox para os topicos de cada servico
- Registrar historico de todas as transicoes (`saga_history`)
- Detectar sagas com timeout via scheduler (a cada 60s, default 5 min)
- Idempotencia: ignorar sagas duplicadas por `order_id`

## Maquina de Estados

```
Happy path:
STARTED -> RESERVING_STOCK -> PROCESSING_PAYMENT -> CONFIRMING_ORDER -> CONFIRMING_RESERVATION -> COMPLETED

Compensacao (estoque insuficiente):
RESERVING_STOCK -> CANCELING_ORDER -> FAILED

Compensacao (pagamento negado):
PROCESSING_PAYMENT -> RELEASING_STOCK -> CANCELING_ORDER -> FAILED
```

| Step Atual | Reply Status | Proximo Step | Comando Emitido |
|---|---|---|---|
| `STARTED` | `CREATED` | `RESERVING_STOCK` | `inventory.commands.reserve-stock` |
| `RESERVING_STOCK` | `SUCCESS` | `PROCESSING_PAYMENT` | `payments.commands.process-payment` |
| `RESERVING_STOCK` | `FAILURE` | `CANCELING_ORDER` | `orders.commands.cancel-order` |
| `PROCESSING_PAYMENT` | `SUCCESS` | `CONFIRMING_ORDER` | `orders.commands.confirm-order` |
| `PROCESSING_PAYMENT` | `FAILURE` | `RELEASING_STOCK` | `inventory.commands.release-stock` |
| `RELEASING_STOCK` | `SUCCESS` | `CANCELING_ORDER` | `orders.commands.cancel-order` |
| `CONFIRMING_ORDER` | `SUCCESS` | `CONFIRMING_RESERVATION` | `inventory.commands.confirm-reservation` |
| `CONFIRMING_RESERVATION` | `SUCCESS` | `COMPLETED` | _(terminal)_ |
| `CANCELING_ORDER` | `SUCCESS` | `FAILED` | _(terminal)_ |

## Topicos Kafka

### Consome (replies)

| Topico | DTO | Consumer |
|--------|-----|----------|
| `orders.replies` | `OrderCreatedReply` | `OrdersReplyConsumer` -> `StartSagaUseCase` |
| `inventory.replies` | `SagaReplyEvent` | `InventoryReplyConsumer` -> `HandleReplyUseCase` |
| `payments.replies` | `SagaReplyEvent` | `PaymentsReplyConsumer` -> `HandleReplyUseCase` |

### Produz (comandos via Outbox)

| Topico | Descricao |
|--------|-----------|
| `inventory.commands.reserve-stock` | Reservar estoque |
| `inventory.commands.release-stock` | Liberar estoque (compensacao) |
| `inventory.commands.confirm-reservation` | Confirmar reserva |
| `payments.commands.process-payment` | Processar pagamento |
| `orders.commands.confirm-order` | Confirmar pedido |
| `orders.commands.cancel-order` | Cancelar pedido |

> Servico interno — nao expoe API publica. Opera exclusivamente via eventos Kafka.

## Configuracao

Configurado via `application.yaml` com override por variaveis de ambiente:

| Propriedade | Default | Descricao |
|-------------|---------|-----------|
| `spring.datasource.url` | `jdbc:postgresql://localhost:5433/saga_db` | URL do PostgreSQL |
| `spring.datasource.username` | `saga` | Usuario do banco |
| `spring.datasource.password` | `saga` | Senha do banco |
| `spring.kafka.bootstrap-servers` | `localhost:29092` | Brokers Kafka |
| `server.port` | `8084` | Porta do servidor |
| `saga.timeout-minutes` | `5` | Timeout para sagas em andamento |

## Build & Run

```bash
./mvnw package                   # Build + testes + JAR
./mvnw package -DskipTests       # Build sem testes
./mvnw test                      # Rodar testes
./mvnw spring-boot:run           # Executar (requer PostgreSQL 5433, Kafka 29092)
```

## Database

PostgreSQL (`saga_db`, porta 5433). Schema em `INIT.sql`. Hibernate `ddl-auto: update`.

- **sagas** — estado da saga com payload JSONB
- **saga_history** — historico de transicoes (step, status, reason)
- **outbox_events** — eventos pendentes para publicacao via relay

## Testes

- `SagaStateMachineTest` — 18 testes cobrindo todas as transicoes (happy path, compensacao, estados terminais)
- `SagaManagerTest` — 5 testes para logica de negocio (iniciar saga, idempotencia, avancar, compensar, estados terminais)
- JUnit 5 + Mockito Kotlin, repositorios mockados

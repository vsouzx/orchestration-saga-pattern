# Saga Orchestrator

Orquestrador central que coordena toda a saga de pedidos. Implementa uma **maquina de estados** que define as transicoes entre steps e emite comandos para os servicos participantes. Consome replies de todos os servicos e avanca ou compensa a saga conforme o resultado. Utiliza **Arquitetura Hexagonal** e **Transactional Outbox**.

**Porta:** 8084 | **Banco:** PostgreSQL | **Linguagem:** Kotlin / Spring Boot 3.4

## Arquitetura (Hexagonal)

```
src/main/kotlin/br/com/souza/saga_orchestrator/
├── adapter/
│   ├── in/
│   │   └── consumer/            # Kafka consumers (replies) - 7 consumers dedicados
│   │       ├── CreateOrderReplyConsumer       # Trata CREATED (start saga)
│   │       ├── ConfirmOrderReplyConsumer      # Reply de pedido confirmado
│   │       ├── CancelOrderReplyConsumer       # Reply de pedido cancelado
│   │       ├── ReserveStockReplyConsumer      # Reply de reserva de estoque
│   │       ├── ReleaseStockReplyConsumer      # Reply de liberacao de estoque
│   │       ├── ConfirmReservationReplyConsumer # Reply de confirmacao de reserva
│   │       └── ProcessPaymentReplyConsumer    # Reply de pagamento
│   └── out/
│       ├── relay/               # OutboxRelayScheduler (polling 1s, batch 50)
│       └── saga/                # Saga & SagaHistory persistence (JPA)
├── application/
│   ├── domain/
│   │   ├── model/               # Saga, SagaStep, ReplyStatus, OutboxEvent
│   │   └── service/
│   │       ├── SagaManager      # Logica de orquestracao (enriquece payload, historico granular)
│   │       └── SagaStateMachine # Tabela de transicoes (step, status) -> (completedStep, nextStep, command, description)
│   └── ports/
│       ├── in/                  # StartSagaUseCase, HandleReplyUseCase
│       └── out/                 # SagaRepositoryPort, SagaHistoryRepositoryPort, OutboxEventRepositoryPort
└── infrastructure/              # Kafka config, OpenTelemetry
```

## Responsabilidades

- Iniciar saga ao receber reply `orders.replies.create-order` (pedido criado)
- Manter estado da saga em banco de dados (PostgreSQL)
- Transicionar entre steps da maquina de estados (granular: `_PENDING`, `_COMPLETED`, `_FAILED`)
- Emitir comandos via Outbox para os topicos de cada servico
- Enriquecer payload com `sagaId` e `reason` antes de emitir comandos
- Registrar historico de todas as transicoes (`saga_history`)
- Processar replies de confirmacao/cancelamento de pedido (consumers dedicados por tipo de evento)
- Idempotencia: ignorar sagas duplicadas por `order_id`

## Maquina de Estados

```
Happy path:
ORDER_CREATED -> RESERVING_STOCK_PENDING -> RESERVING_STOCK_COMPLETED -> PROCESSING_PAYMENT_PENDING ->
PROCESSING_PAYMENT_COMPLETED -> CONFIRMING_ORDER_PENDING -> CONFIRMING_ORDER_COMPLETED ->
CONFIRMING_RESERVATION_PENDING -> CONFIRMING_RESERVATION_COMPLETED -> ORDER_COMPLETED

Compensacao (estoque insuficiente):
ORDER_CREATED -> RESERVING_STOCK_PENDING -> RESERVING_STOCK_FAILED -> CANCELING_ORDER_PENDING ->
CANCELING_ORDER_COMPLETED -> ORDER_FAILED

Compensacao (pagamento negado):
... -> RESERVING_STOCK_COMPLETED -> PROCESSING_PAYMENT_PENDING -> PROCESSING_PAYMENT_FAILED ->
RELEASING_STOCK_PENDING -> RELEASING_STOCK_COMPLETED -> CANCELING_ORDER_PENDING ->
CANCELING_ORDER_COMPLETED -> ORDER_FAILED
```

Cada transicao registra o step completado (`completedStep`) e o proximo step (`nextStep`), com uma `description` descritiva para logging.

| Step Atual | Reply Status | Step Completado | Proximo Step | Comando Emitido |
|---|---|---|---|---|
| `ORDER_CREATED` | `CREATED` | `ORDER_CREATED` | `RESERVING_STOCK_PENDING` | `inventory.commands.reserve-stock` |
| `RESERVING_STOCK_PENDING` | `SUCCESS` | `RESERVING_STOCK_COMPLETED` | `PROCESSING_PAYMENT_PENDING` | `payments.commands.process-payment` |
| `RESERVING_STOCK_PENDING` | `FAILURE` | `RESERVING_STOCK_FAILED` | `CANCELING_ORDER_PENDING` | `orders.commands.cancel-order` |
| `PROCESSING_PAYMENT_PENDING` | `SUCCESS` | `PROCESSING_PAYMENT_COMPLETED` | `CONFIRMING_ORDER_PENDING` | `orders.commands.confirm-order` |
| `PROCESSING_PAYMENT_PENDING` | `FAILURE` | `PROCESSING_PAYMENT_FAILED` | `RELEASING_STOCK_PENDING` | `inventory.commands.release-stock` |
| `RELEASING_STOCK_PENDING` | `SUCCESS` | `RELEASING_STOCK_COMPLETED` | `CANCELING_ORDER_PENDING` | `orders.commands.cancel-order` |
| `CONFIRMING_ORDER_PENDING` | `SUCCESS` | `CONFIRMING_ORDER_COMPLETED` | `CONFIRMING_RESERVATION_PENDING` | `inventory.commands.confirm-reservation` |
| `CONFIRMING_RESERVATION_PENDING` | `SUCCESS` | `CONFIRMING_RESERVATION_COMPLETED` | `ORDER_COMPLETED` | _(terminal)_ |
| `CANCELING_ORDER_PENDING` | `SUCCESS` | `CANCELING_ORDER_COMPLETED` | `ORDER_FAILED` | _(terminal)_ |

## Topicos Kafka

### Consome (replies) - 7 consumers dedicados

| Topico | DTO | Consumer |
|--------|-----|----------|
| `orders.replies.create-order` | `OrderCreatedReply` | `CreateOrderReplyConsumer` -> `StartSagaUseCase` |
| `orders.replies.confirm-order` | `SagaReplyEvent` | `ConfirmOrderReplyConsumer` -> `HandleReplyUseCase` |
| `orders.replies.cancel-order` | `SagaReplyEvent` | `CancelOrderReplyConsumer` -> `HandleReplyUseCase` |
| `inventory.replies.reserve-stock` | `SagaReplyEvent` | `ReserveStockReplyConsumer` -> `HandleReplyUseCase` |
| `inventory.replies.release-stock` | `SagaReplyEvent` | `ReleaseStockReplyConsumer` -> `HandleReplyUseCase` |
| `inventory.replies.confirm-reservation` | `SagaReplyEvent` | `ConfirmReservationReplyConsumer` -> `HandleReplyUseCase` |
| `payments.replies.process-payment` | `SagaReplyEvent` | `ProcessPaymentReplyConsumer` -> `HandleReplyUseCase` |

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
- **saga_history** — historico de transicoes (step, reason)
- **outbox_events** — eventos pendentes para publicacao via relay

## Testes

- `SagaStateMachineTest` — 18 testes cobrindo todas as transicoes (happy path, compensacao, estados terminais)
- `SagaManagerTest` — 5 testes para logica de negocio (iniciar saga, idempotencia, avancar, compensar, estados terminais)
- JUnit 5 + Mockito Kotlin, repositorios mockados

# Saga Orchestrator — Design Spec

## Contexto

O projeto possui 3 servicos (orders-service, payments-service, inventory-service) que se comunicam via eventos Kafka seguindo o pattern saga coreografado com Transactional Outbox. Esta refatoracao cria um novo servico `saga-orchestrator` para centralizar a coordenacao da saga, migrando de coreografia para orquestracao.

## Decisoes de Design

| Decisao | Escolha |
|---|---|
| Linguagem do orchestrator | Kotlin / Spring Boot 3.4 |
| Comunicacao | Kafka command/reply (assincrono) |
| Persistencia de estado | State machine no PostgreSQL |
| Banco de dados | PostgreSQL (instancia dedicada, porta 5433) |
| Outbox pattern | Polling relay (`@Scheduled`, mesmo padrao do inventory) |
| Arquitetura | Hexagonal (ports & adapters) |
| Papel dos servicos | Command handlers puros (nao conhecem a saga) |

---

## 1. Maquina de Estados

### Estados

| Estado | Descricao |
|---|---|
| `STARTED` | Saga criada, pronta para enviar primeiro comando |
| `RESERVING_STOCK` | Comando `reserve-stock` enviado ao inventory |
| `PROCESSING_PAYMENT` | Comando `process-payment` enviado ao payments |
| `CONFIRMING_ORDER` | Comando `confirm-order` enviado ao orders |
| `CONFIRMING_RESERVATION` | Comando `confirm-reservation` enviado ao inventory |
| `COMPLETED` | Saga finalizada com sucesso |
| `RELEASING_STOCK` | Compensacao: comando `release-stock` enviado ao inventory |
| `CANCELING_ORDER` | Compensacao: comando `cancel-order` enviado ao orders |
| `FAILED` | Saga finalizada com falha (compensacao concluida) |

### Transicoes

**Happy path:**
```
STARTED → RESERVING_STOCK → PROCESSING_PAYMENT → CONFIRMING_ORDER → CONFIRMING_RESERVATION → COMPLETED
```

**Compensacao a partir de RESERVING_STOCK (estoque insuficiente):**
```
RESERVING_STOCK + FAILURE → CANCELING_ORDER → FAILED
```

**Compensacao a partir de PROCESSING_PAYMENT (pagamento negado):**
```
PROCESSING_PAYMENT + FAILURE → RELEASING_STOCK → CANCELING_ORDER → FAILED
```

---

## 2. Topicos Kafka

### Topicos de comando (orchestrator → servicos)

| Topico | Produtor | Consumidor | Payload |
|---|---|---|---|
| `inventory.commands.reserve-stock` | Orchestrator | Inventory | `{sagaId, orderId, productId, quantity}` |
| `inventory.commands.release-stock` | Orchestrator | Inventory | `{sagaId, orderId, productId, quantity}` |
| `inventory.commands.confirm-reservation` | Orchestrator | Inventory | `{sagaId, orderId}` |
| `payments.commands.process-payment` | Orchestrator | Payments | `{sagaId, orderId, amount, paymentType}` |
| `orders.commands.confirm-order` | Orchestrator | Orders | `{sagaId, orderId}` |
| `orders.commands.cancel-order` | Orchestrator | Orders | `{sagaId, orderId, reason}` |

### Topicos de reply (servicos → orchestrator)

| Topico | Produtor | Consumidor | Payload |
|---|---|---|---|
| `inventory.replies` | Inventory | Orchestrator | `{sagaId, orderId, status: SUCCESS/FAILURE, reason?}` |
| `payments.replies` | Payments | Orchestrator | `{sagaId, orderId, status: SUCCESS/FAILURE, reason?}` |
| `orders.replies` | Orders | Orchestrator | `{sagaId, orderId, status: SUCCESS/FAILURE}` |

### Topicos removidos (coreografia)

- `orders.created`, `orders.confirmed`
- `inventory.reserved`, `inventory.insufficient-stock`, `inventory.released`
- `payments.authorized`, `payments.denied`

---

## 3. Saga Orchestrator — Estrutura

### Arquitetura hexagonal

```
saga-orchestrator/
├── src/main/kotlin/br/com/souza/saga_orchestrator/
│   ├── adapter/
│   │   ├── in/
│   │   │   └── consumer/           # Kafka consumers (replies de cada servico)
│   │   └── out/
│   │       ├── saga/               # SagaRepository (JPA/PostgreSQL)
│   │       └── relay/              # OutboxRelayScheduler
│   ├── application/
│   │   ├── domain/
│   │   │   ├── model/              # Saga, SagaStep (enums), SagaHistory
│   │   │   └── service/            # SagaStateMachine, SagaManager
│   │   └── ports/
│   │       ├── in/                 # StartSagaUseCase, HandleReplyUseCase
│   │       └── out/                # SagaRepositoryPort, OutboxRepositoryPort
│   └── infrastructure/
│       └── config/                 # Kafka, DB, OTel configs
├── INIT.sql
├── Dockerfile
└── pom.xml
```

### Componentes principais

**`SagaStateMachine`** — nucleo da logica:
- Define todas as transicoes validas (estado atual + evento → proximo estado + comando a enviar)
- `handleReply(saga, replyStatus)` → retorna proximo estado e comando
- `getCompensationStep(saga)` → retorna o comando de compensacao baseado no estado atual
- Pura logica, sem I/O — facil de testar unitariamente

**`SagaManager`** — orquestra o fluxo:
- `startSaga(orderId, payload)` — cria saga em `STARTED`, insere registro em `saga_history`, processa primeira transicao, persiste saga + outbox event na mesma transacao
- `onReply(sagaId, status, reason?)` — carrega saga, delega para `SagaStateMachine`, persiste novo estado + insere registro em `saga_history` + proximo comando via outbox, tudo na mesma transacao

**`ReplyConsumer`** — consumers Kafka:
- Um consumer por topico de reply (`inventory.replies`, `payments.replies`, `orders.replies`)
- Extrai `sagaId` e `status` do payload, delega para `SagaManager.onReply()`

**`OutboxRelayScheduler`** — mesmo padrao do inventory:
- `@Scheduled(fixedDelay = 1000)` polling de eventos pendentes
- Publica nos topicos de comando correspondentes

### Schema do banco (PostgreSQL)

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
    payload JSONB NOT NULL,
    trace_parent VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retries_count INT DEFAULT 0,
    max_retries INT DEFAULT 5,
    created_at TIMESTAMP DEFAULT NOW(),
    sent_at TIMESTAMP,
    locked_at TIMESTAMP
);
```

---

## 4. Mudancas nos Servicos Existentes

### Orders Service (Go)

**Remover:**
- `PaymentsConsumer` (consumia `payments.authorized`)
- `InventoryConsumer` (consumia `inventory.insufficient-stock`)
- `InventoryReleasedConsumer` (consumia `inventory.released`)
- Outbox events de `orders.created` e `orders.confirmed`

**Adicionar:**
- Consumer para `orders.commands.confirm-order` — atualiza order para `CONFIRMED`, publica reply em `orders.replies`
- Consumer para `orders.commands.cancel-order` — atualiza order para `CANCELED` com reason, publica reply em `orders.replies`

**Alterar:**
- `POST /v1/orders` — apos criar order `PENDING`, publica reply em `orders.replies` com `{orderId, status: CREATED}` para o orchestrator iniciar a saga

**Manter:**
- Transactional Outbox + Relay (agora publica replies)
- Idempotencia via Redis
- Endpoints `GET /v1/orders`, `GET /health`

### Payments Service (Go)

**Remover:**
- `InventoryConsumer` (consumia `inventory.reserved`)
- Outbox events de `payments.authorized` e `payments.denied`

**Adicionar:**
- Consumer para `payments.commands.process-payment` — avalia regras, cria payment record, publica reply em `payments.replies` com `SUCCESS` ou `FAILURE`

**Manter:**
- Regras de negocio (BOLETO → denied, CREDIT_CARD > 10000 → denied)
- Transactional Outbox + Relay (agora publica replies)
- Idempotencia via `UNIQUE(order_id)`

### Inventory Service (Kotlin/Spring Boot)

**Remover:**
- `OrderCreatedKafkaConsumer` (consumia `orders.created`)
- `PaymentsDeniedKafkaConsumer` (consumia `payments.denied`)
- `OrderConfirmedKafkaConsumer` (consumia `orders.confirmed`)
- Outbox events de `inventory.reserved`, `inventory.insufficient-stock`, `inventory.released`

**Adicionar:**
- Consumer para `inventory.commands.reserve-stock` — executa reserva, responde em `inventory.replies` com `SUCCESS` ou `FAILURE`
- Consumer para `inventory.commands.release-stock` — libera estoque, responde em `inventory.replies` com `SUCCESS`
- Consumer para `inventory.commands.confirm-reservation` — confirma reservacao, responde em `inventory.replies` com `SUCCESS`

**Manter:**
- Logica de reserva/liberacao com `SELECT FOR UPDATE`
- Transactional Outbox + Relay (agora publica replies)
- Endpoints REST de produtos/estoque

---

## 5. Observabilidade

O orchestrator segue o mesmo padrao do inventory-service:

- **Logging:** SLF4J/Logback com Logstash encoder (JSON), MDC com traceId/spanId
- **Tracing:** OpenTelemetry Spring Boot starter, trace context propagado via header `traceparent` nos messages Kafka
- **Metricas:** Micrometer + Prometheus (sagas iniciadas, completadas, falhadas, tempo medio por step)

---

## 6. Docker Compose

**Adicionar:**
- Container `saga-orchestrator` (porta 8084)
- Container `postgres-orchestrator` (porta 5433)

**Atualizar:**
- `kafka-init` — adicionar 9 novos topicos (6 commands + 3 replies), remover 7 topicos antigos

---

## 7. Trigger da Saga

O orders-service, ao criar um order via `POST /v1/orders`, publica um reply em `orders.replies` com `{orderId, userId, productId, quantity, paymentType, status: CREATED}`. O orchestrator consome esse reply e inicia a saga chamando `SagaManager.startSaga()`, persistindo esses dados no campo `payload` da saga para uso nos comandos subsequentes (ex: `productId` e `quantity` para `reserve-stock`, `amount` e `paymentType` para `process-payment`).

## 8. Timeout Handling

Um `@Scheduled` job verifica sagas em estado nao-terminal ha mais de 5 minutos (configuravel via property `saga.timeout-minutes`). Sagas em timeout entram em fluxo de compensacao.

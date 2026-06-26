# Orders Service

Ponto de entrada do sistema de pedidos distribuido. Recebe requisicoes HTTP para criacao de pedidos e publica replies para o orquestrador via **Transactional Outbox**. Consome comandos do orquestrador para confirmar ou cancelar pedidos.

**Porta:** 8081 | **Banco:** MySQL | **Linguagem:** Go 1.25

## Arquitetura

```
orders-service/
├── cmd/api/main.go              # Entrypoint
├── internal/
│   ├── config/                  # Configuracao via env vars (Viper + config.yaml)
│   ├── handler/                 # HTTP handlers (Gin)
│   ├── service/                 # Logica de negocio (CreateOrder, ConfirmOrder, CancelOrder)
│   ├── repository/              # Acesso a dados MySQL (interface DBTX para testabilidade)
│   ├── domain/                  # Modelos, enums, payloads de eventos e AppError
│   ├── consumer/                # Kafka consumers (comandos do orquestrador)
│   │   ├── confirm_order.go     # Consome orders.commands.confirm-order
│   │   ├── cancel_order.go      # Consome orders.commands.cancel-order
│   │   └── trace_context.go     # Extracao de traceparent (W3C)
│   ├── relay/                   # Outbox relay (polling a cada 5s, FOR UPDATE SKIP LOCKED)
│   ├── middleware/              # IdempotencyMiddleware (Redis SetNX) e ErrorHandler
│   └── logger/                  # Logger context-aware (Zap + trace_id/span_id)
├── INIT.sql                     # Schema do banco
├── Dockerfile
└── go.mod
```

## Responsabilidades

- Criar pedidos com status `PENDING`
- Publicar reply `orders.replies` via Outbox (notifica o orquestrador)
- Confirmar pedido (`CONFIRMED`) ao receber comando `orders.commands.confirm-order`
- Cancelar pedido (`CANCELED`) ao receber comando `orders.commands.cancel-order`
- Garantir idempotencia via Redis (`Idempotency-Key` header) + constraint UNIQUE no MySQL

## API

| Metodo | Rota | Middleware | Descricao |
|--------|------|------------|-----------|
| POST | `/v1/orders` | IdempotencyMiddleware, ErrorHandler | Cria um pedido (retorna 202) |
| GET | `/v1/orders` | ErrorHandler | Lista todos os pedidos |
| GET | `/health` | — | Health check |

### Criar Pedido

```bash
curl -X POST http://localhost:8081/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "productId": 1,
    "quantity": 2,
    "paymentType": "PIX"
  }'
```

> Requer header `Idempotency-Key` (retorna 400 se ausente, 409 se duplicado).

## Topicos Kafka

| Direcao | Topico | Descricao |
|---------|--------|-----------|
| Consome | `orders.commands.confirm-order` | Comando do orquestrador para confirmar pedido |
| Consome | `orders.commands.cancel-order` | Comando do orquestrador para cancelar pedido |
| Produz (via Outbox) | `orders.replies` | Reply de pedido criado/confirmado/cancelado |

## Configuracao

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `SERVER_PORT` | `:8081` | Porta do servidor HTTP |
| `MYSQL_DSN` | `root:root@tcp(localhost:3307)/orders?parseTime=true` | DSN do MySQL |
| `REDIS_ADDR` | `localhost:6379` | Endereco do Redis |
| `REDIS_PASS` | (vazio) | Senha do Redis |
| `REDIS_DB` | `0` | Database do Redis |
| `KAFKA_BROKERS` | `localhost:29092` | Brokers Kafka |
| `KAFKA_ORDERS_REPLIES_TOPIC` | `orders.replies` | Topico de replies |
| `KAFKA_CONFIRM_ORDER_TOPIC` | `orders.commands.confirm-order` | Topico de comando confirmar |
| `KAFKA_CANCEL_ORDER_TOPIC` | `orders.commands.cancel-order` | Topico de comando cancelar |
| `OUTBOX_BATCH_SIZE` | `10` | Tamanho do batch do relay |
| `OTEL_EXPORTER_ENDPOINT` | `localhost:4317` | Endpoint gRPC do OTel Collector |

## Build & Run

```bash
go build ./...           # Compilar
go run ./cmd/api         # Executar (requer MySQL 3307, Redis, Kafka 29092)
go test ./...            # Testes
go vet ./...             # Lint
```

## Database

MySQL (`orders`, porta 3307). Schema em `INIT.sql`.

- **orders** — pedidos com status (`PENDING`, `CONFIRMED`, `CANCELED`), `idempotency_key` UNIQUE
- **outbox** — eventos pendentes para publicacao via relay (`PENDING`, `PROCESSING`, `SENT`, `FAILED`, `DEAD_LETTER`)

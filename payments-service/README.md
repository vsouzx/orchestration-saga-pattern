# Payments Service

Microservico de pagamentos que opera exclusivamente via comandos do orquestrador recebidos pelo Kafka. Processa pagamentos com base em regras de negocio e publica replies via **Transactional Outbox**.

**Porta:** 8083 | **Banco:** MySQL | **Linguagem:** Go 1.25

## Arquitetura

```
payments-service/
├── cmd/api/main.go              # Entrypoint
├── internal/
│   ├── config/                  # Configuracao via env vars (Viper + config.yaml)
│   ├── handler/                 # HTTP handlers (Gin)
│   ├── service/                 # Logica de avaliacao de pagamento
│   ├── repository/              # Acesso a dados MySQL (interface DBTX)
│   ├── domain/                  # Modelos, enums e payloads de eventos
│   ├── consumer/                # Kafka consumer (comando do orquestrador)
│   ├── relay/                   # Outbox relay (polling)
│   └── logger/                  # Logger context-aware (Zap + trace_id/span_id)
├── INIT.sql                     # Schema do banco
├── Dockerfile
└── go.mod
```

## Responsabilidades

- Consumir comando `payments.commands.process-payment` e processar pagamento
- Avaliar regras de pagamento (ex: negar BOLETO, negar cartao > 10.000)
- Publicar reply em `payments.replies` com status `SUCCESS` ou `FAILURE` via Outbox
- Idempotencia via constraint de banco (`order_id UNIQUE`)

## API

| Metodo | Rota | Descricao |
|--------|------|-----------|
| GET | `/health` | Health check |

> O Payments Service nao expoe endpoints de criacao — opera exclusivamente via comandos do orquestrador.

## Topicos Kafka

| Direcao | Topico | Descricao |
|---------|--------|-----------|
| Consome | `payments.commands.process-payment` | Comando do orquestrador para processar pagamento |
| Produz (via Outbox) | `payments.replies` | Reply com status SUCCESS ou FAILURE |

## Configuracao

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `SERVER_PORT` | `:8083` | Porta do servidor HTTP |
| `MYSQL_DSN` | `root:root@tcp(localhost:3308)/payments?parseTime=true` | DSN do MySQL |
| `KAFKA_BROKERS` | `localhost:29092` | Brokers Kafka |
| `KAFKA_PROCESS_PAYMENT_TOPIC` | `payments.commands.process-payment` | Topico de comando |
| `KAFKA_PAYMENTS_REPLIES_TOPIC` | `payments.replies` | Topico de replies |
| `OUTBOX_BATCH_SIZE` | `10` | Tamanho do batch do relay |
| `OTEL_EXPORTER_ENDPOINT` | `localhost:4317` | Endpoint gRPC do OTel Collector |

## Build & Run

```bash
go build ./...           # Compilar
go run ./cmd/api         # Executar (requer MySQL 3308, Kafka 29092)
go test ./...            # Testes
go vet ./...             # Lint
```

## Database

MySQL (`payments`, porta 3308). Schema em `INIT.sql`.

- **payments** — pagamentos com `order_id` UNIQUE, status (`AUTHORIZED`, `DENIED`)
- **outbox** — eventos pendentes para publicacao via relay

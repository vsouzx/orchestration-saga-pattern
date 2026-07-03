# Inventory Service

Microservico de gerenciamento de produtos, estoque e reservas. Implementa **Arquitetura Hexagonal** (ports & adapters) e participa da saga de orquestracao consumindo comandos do orquestrador e publicando replies via **Transactional Outbox**.

**Porta:** 8082 | **Banco:** PostgreSQL | **Linguagem:** Kotlin / Spring Boot 3.4

## Arquitetura (Hexagonal)

```
src/main/kotlin/br/com/souza/inventory_service/
├── adapter/
│   ├── in/
│   │   ├── web/                 # REST controllers (ProductController)
│   │   └── consumer/
│   │       └── saga/            # Consumers de comandos do orquestrador
│   │           ├── ReserveStockCommandConsumer
│   │           ├── ReleaseStockCommandConsumer
│   │           └── ConfirmReservationCommandConsumer
│   └── out/
│       ├── product/             # Product persistence (JPA)
│       ├── stock/               # Stock persistence (JPA)
│       ├── reservation/         # Reservation persistence (JPA)
│       ├── relay/               # OutboxRelayScheduler (polling 1s, batch 50)
│       └── models/              # JPA entities
├── application/
│   ├── domain/
│   │   ├── model/               # Domain models (Product, Stock, StockReservation, OutboxEvent)
│   │   └── service/             # Use cases (ReserveStock, ReleaseStock, ConfirmReservation)
│   └── ports/
│       ├── in/                  # Input ports (use case interfaces)
│       └── out/                 # Output ports (repository interfaces)
└── infrastructure/              # Kafka config, OpenTelemetry
```

## Responsabilidades

- CRUD de produtos e estoque
- Reservar estoque ao receber comando `inventory.commands.reserve-stock`
- Liberar estoque (compensacao) ao receber comando `inventory.commands.release-stock`
- Confirmar reserva ao receber comando `inventory.commands.confirm-reservation`
- Publicar replies em `inventory.replies` com status `SUCCESS` ou `FAILURE`
- Pessimistic locking (`SELECT FOR UPDATE`) para controle de concorrencia

## API

| Metodo | Rota | Descricao |
|--------|------|-----------|
| GET | `/v1/products` | Lista todos os produtos |
| POST | `/v1/products` | Cria um produto |
| GET | `/v1/products/stocks` | Lista todos os estoques |
| POST | `/v1/products/{id}/stock` | Cria estoque para um produto |
| PATCH | `/v1/products/{id}/stock/quantity` | Atualiza quantidade do estoque |

## Topicos Kafka

| Direcao | Topico | Descricao |
|---------|--------|-----------|
| Consome | `inventory.commands.reserve-stock` | Comando para reservar estoque |
| Consome | `inventory.commands.release-stock` | Comando para liberar estoque (compensacao) |
| Consome | `inventory.commands.confirm-reservation` | Comando para confirmar reserva |
| Produz (via Outbox) | `inventory.replies` | Reply com status SUCCESS ou FAILURE (event types: `INVENTORY_RESERVED_REPLY`, `INVENTORY_RELEASED_REPLY`, `INVENTORY_CONFIRMED_REPLY`) |

## Configuracao

Configurado via `application.yaml` com override por variaveis de ambiente:

| Propriedade | Default | Descricao |
|-------------|---------|-----------|
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/inventory_db` | URL do PostgreSQL |
| `spring.datasource.username` | `inventory` | Usuario do banco |
| `spring.datasource.password` | `inventory` | Senha do banco |
| `spring.kafka.bootstrap-servers` | `localhost:29092` | Brokers Kafka |
| `server.port` | `8082` | Porta do servidor |

## Build & Run

```bash
./mvnw package                   # Build + testes + JAR
./mvnw package -DskipTests       # Build sem testes
./mvnw test                      # Rodar testes
./mvnw spring-boot:run           # Executar (requer PostgreSQL 5432, Kafka 29092)
```

## Database

PostgreSQL (`inventory_db`, porta 5432). Schema em `INIT.sql`. Hibernate `ddl-auto: update`.

- **products** — cadastro de produtos com preco em centavos
- **stocks** — estoque por produto (UNIQUE)
- **stock_reservations** — reservas por pedido (status: `RESERVED`, `CONFIRMED`, `RELEASED`)
- **outbox_events** — eventos pendentes para publicacao via relay

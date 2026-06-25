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

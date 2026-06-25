package br.com.souza.saga_orchestrator.application.domain.model

enum class OutboxEventStatus {
    PENDING,
    PROCESSING,
    SENT,
    FAILED,
    DEAD_LETTER
}

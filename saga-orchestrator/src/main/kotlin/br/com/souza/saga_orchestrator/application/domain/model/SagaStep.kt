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

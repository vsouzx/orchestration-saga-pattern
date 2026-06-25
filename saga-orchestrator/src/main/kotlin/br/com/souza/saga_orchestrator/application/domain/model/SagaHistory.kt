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

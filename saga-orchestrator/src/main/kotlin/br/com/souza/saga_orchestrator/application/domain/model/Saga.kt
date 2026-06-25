package br.com.souza.saga_orchestrator.application.domain.model

import java.time.LocalDateTime

data class Saga(
    val id: String,
    val orderId: String,
    val currentStep: SagaStep,
    val payload: String,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val updatedAt: LocalDateTime = LocalDateTime.now()
)

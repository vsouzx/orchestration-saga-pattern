package br.com.souza.saga_orchestrator.adapter.out.saga.mappers

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaJpaEntity
import br.com.souza.saga_orchestrator.application.domain.model.Saga
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

fun Saga.toJpaEntity() = SagaJpaEntity(
    id = id,
    orderId = orderId,
    currentStep = currentStep.name,
    payload = payload,
    createdAt = createdAt,
    updatedAt = updatedAt
)

fun SagaJpaEntity.toDomain() = Saga(
    id = id,
    orderId = orderId,
    currentStep = SagaStep.valueOf(currentStep),
    payload = payload,
    createdAt = createdAt,
    updatedAt = updatedAt
)

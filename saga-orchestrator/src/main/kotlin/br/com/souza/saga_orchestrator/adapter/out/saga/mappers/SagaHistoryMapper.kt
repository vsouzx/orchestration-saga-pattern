package br.com.souza.saga_orchestrator.adapter.out.saga.mappers

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaHistoryJpaEntity
import br.com.souza.saga_orchestrator.application.domain.model.SagaHistory
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

fun SagaHistory.toJpaEntity() = SagaHistoryJpaEntity(
    id = id,
    sagaId = sagaId,
    step = step.name,
    status = status,
    reason = reason,
    createdAt = createdAt
)

fun SagaHistoryJpaEntity.toDomain() = SagaHistory(
    id = id,
    sagaId = sagaId,
    step = SagaStep.valueOf(step),
    status = status,
    reason = reason,
    createdAt = createdAt
)

package br.com.souza.saga_orchestrator.adapter.out.relay.mappers

import br.com.souza.saga_orchestrator.adapter.out.relay.models.OutboxEventJpaEntity
import br.com.souza.saga_orchestrator.application.domain.model.OutboxEvent
import br.com.souza.saga_orchestrator.application.domain.model.OutboxEventStatus

fun OutboxEvent.toJpaEntity() = OutboxEventJpaEntity(
    id = id ?: "",
    aggregateId = aggregateId,
    aggregateType = aggregateType,
    eventType = eventType,
    topic = topic,
    payload = payload,
    traceParent = traceParent,
    status = status.name,
    retriesCount = retriesCount,
    maxRetries = maxRetries,
    createdAt = createdAt,
    sentAt = sentAt,
    lockedAt = lockedAt
)

fun OutboxEventJpaEntity.toDomain() = OutboxEvent(
    id = id,
    aggregateId = aggregateId,
    aggregateType = aggregateType,
    eventType = eventType,
    topic = topic,
    payload = payload,
    traceParent = traceParent,
    status = OutboxEventStatus.valueOf(status),
    retriesCount = retriesCount,
    maxRetries = maxRetries,
    createdAt = createdAt,
    sentAt = sentAt,
    lockedAt = lockedAt
)

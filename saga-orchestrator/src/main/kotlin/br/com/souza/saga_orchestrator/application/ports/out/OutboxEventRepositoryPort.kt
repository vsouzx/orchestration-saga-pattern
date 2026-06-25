package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.OutboxEvent

interface OutboxEventRepositoryPort {
    fun save(event: OutboxEvent): OutboxEvent
    fun findPendingEvents(limit: Int): List<OutboxEvent>
    fun lockEvents(ids: List<String>): Int
    fun markAsSent(id: String)
    fun markAsFailed(id: String)
    fun markAsDeadLetter(id: String)
}

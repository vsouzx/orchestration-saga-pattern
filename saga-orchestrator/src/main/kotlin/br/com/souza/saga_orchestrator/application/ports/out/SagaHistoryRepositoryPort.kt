package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.SagaHistory

interface SagaHistoryRepositoryPort {
    fun save(history: SagaHistory): SagaHistory
    fun findBySagaId(sagaId: String): List<SagaHistory>
}

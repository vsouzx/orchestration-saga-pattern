package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.Saga

interface SagaRepositoryPort {
    fun save(saga: Saga): Saga
    fun findById(id: String): Saga?
    fun findByIdForUpdate(id: String): Saga?
    fun findByOrderId(orderId: String): Saga?
}

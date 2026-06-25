package br.com.souza.saga_orchestrator.application.ports.out

import br.com.souza.saga_orchestrator.application.domain.model.Saga
import java.time.LocalDateTime

interface SagaRepositoryPort {
    fun save(saga: Saga): Saga
    fun findById(id: String): Saga?
    fun findByOrderId(orderId: String): Saga?
    fun findTimedOutSagas(olderThan: LocalDateTime): List<Saga>
}

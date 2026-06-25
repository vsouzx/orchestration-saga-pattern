package br.com.souza.saga_orchestrator.adapter.out.saga

import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toDomain
import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toJpaEntity
import br.com.souza.saga_orchestrator.adapter.out.saga.repository.SagaHistoryJpaRepository
import br.com.souza.saga_orchestrator.application.domain.model.SagaHistory
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import org.springframework.stereotype.Component

@Component
class SagaHistoryRepositoryAdapter(
    private val jpaRepository: SagaHistoryJpaRepository
) : SagaHistoryRepositoryPort {

    override fun save(history: SagaHistory): SagaHistory =
        jpaRepository.save(history.toJpaEntity()).toDomain()

    override fun findBySagaId(sagaId: String): List<SagaHistory> =
        jpaRepository.findBySagaIdOrderByCreatedAtAsc(sagaId).map { it.toDomain() }
}

package br.com.souza.saga_orchestrator.adapter.out.saga.repository

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaHistoryJpaEntity
import org.springframework.data.jpa.repository.JpaRepository

interface SagaHistoryJpaRepository : JpaRepository<SagaHistoryJpaEntity, String> {
    fun findBySagaIdOrderByCreatedAtAsc(sagaId: String): List<SagaHistoryJpaEntity>
}

package br.com.souza.saga_orchestrator.adapter.out.saga.repository

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaJpaEntity
import org.springframework.data.jpa.repository.JpaRepository

interface SagaJpaRepository : JpaRepository<SagaJpaEntity, String> {
    fun findByOrderId(orderId: String): SagaJpaEntity?
}

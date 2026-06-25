package br.com.souza.saga_orchestrator.adapter.out.saga.repository

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaJpaEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.LocalDateTime

interface SagaJpaRepository : JpaRepository<SagaJpaEntity, String> {
    fun findByOrderId(orderId: String): SagaJpaEntity?

    @Query("""
        SELECT s FROM SagaJpaEntity s
        WHERE s.updatedAt < :olderThan
        AND s.currentStep NOT IN ('COMPLETED', 'FAILED')
    """)
    fun findTimedOutSagas(@Param("olderThan") olderThan: LocalDateTime): List<SagaJpaEntity>
}

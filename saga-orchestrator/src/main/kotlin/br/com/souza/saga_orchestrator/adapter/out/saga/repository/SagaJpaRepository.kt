package br.com.souza.saga_orchestrator.adapter.out.saga.repository

import br.com.souza.saga_orchestrator.adapter.out.saga.models.SagaJpaEntity
import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import java.util.Optional

interface SagaJpaRepository : JpaRepository<SagaJpaEntity, String> {
    fun findByOrderId(orderId: String): SagaJpaEntity?

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaJpaEntity s WHERE s.id = :id")
    fun findByIdForUpdate(id: String): Optional<SagaJpaEntity>
}

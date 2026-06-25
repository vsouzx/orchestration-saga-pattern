package br.com.souza.saga_orchestrator.adapter.out.saga.models

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "sagas")
data class SagaJpaEntity(
    @Id
    val id: String = "",

    @Column(name = "order_id", nullable = false)
    val orderId: String = "",

    @Column(name = "current_step", nullable = false)
    val currentStep: String = "",

    @Column(nullable = false, columnDefinition = "JSONB")
    val payload: String = "",

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime = LocalDateTime.now()
)

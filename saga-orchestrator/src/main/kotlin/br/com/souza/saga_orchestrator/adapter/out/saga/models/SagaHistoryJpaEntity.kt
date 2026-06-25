package br.com.souza.saga_orchestrator.adapter.out.saga.models

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "saga_history")
data class SagaHistoryJpaEntity(
    @Id
    val id: String = "",

    @Column(name = "saga_id", nullable = false)
    val sagaId: String = "",

    @Column(nullable = false)
    val step: String = "",

    @Column(nullable = false)
    val status: String = "",

    val reason: String? = null,

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now()
)

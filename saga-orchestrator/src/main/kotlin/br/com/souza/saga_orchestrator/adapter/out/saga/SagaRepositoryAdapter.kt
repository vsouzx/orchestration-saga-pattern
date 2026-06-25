package br.com.souza.saga_orchestrator.adapter.out.saga

import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toDomain
import br.com.souza.saga_orchestrator.adapter.out.saga.mappers.toJpaEntity
import br.com.souza.saga_orchestrator.adapter.out.saga.repository.SagaJpaRepository
import br.com.souza.saga_orchestrator.application.domain.model.Saga
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class SagaRepositoryAdapter(
    private val jpaRepository: SagaJpaRepository
) : SagaRepositoryPort {

    override fun save(saga: Saga): Saga =
        jpaRepository.save(saga.toJpaEntity()).toDomain()

    override fun findById(id: String): Saga? =
        jpaRepository.findById(id).orElse(null)?.toDomain()

    override fun findByOrderId(orderId: String): Saga? =
        jpaRepository.findByOrderId(orderId)?.toDomain()

    override fun findTimedOutSagas(olderThan: LocalDateTime): List<Saga> =
        jpaRepository.findTimedOutSagas(olderThan).map { it.toDomain() }
}

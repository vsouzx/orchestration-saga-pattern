package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.*
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.application.ports.`in`.StartSagaUseCase
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.util.UUID

@Service
class SagaManager(
    private val sagaRepository: SagaRepositoryPort,
    private val sagaHistoryRepository: SagaHistoryRepositoryPort,
    private val outboxRepository: OutboxEventRepositoryPort,
    private val stateMachine: SagaStateMachine
) : StartSagaUseCase, HandleReplyUseCase {

    private val logger = LoggerFactory.getLogger(SagaManager::class.java)

    @Transactional
    override fun execute(orderId: String, payload: String, traceParent: String?) {
        // Idempotency: skip if saga already exists for this order
        if (sagaRepository.findByOrderId(orderId) != null) {
            logger.info("Saga already exists for order, skipping", kv("order_id", orderId))
            return
        }

        val sagaId = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        // Create saga in STARTED state
        val saga = Saga(
            id = sagaId,
            orderId = orderId,
            currentStep = SagaStep.STARTED,
            payload = payload,
            createdAt = now,
            updatedAt = now
        )
        sagaRepository.save(saga)
        logger.info("Saga created", kv("saga_id", sagaId), kv("order_id", orderId))

        // Record initial history
        sagaHistoryRepository.save(
            SagaHistory(
                id = UUID.randomUUID().toString(),
                sagaId = sagaId,
                step = SagaStep.STARTED,
                status = "CREATED",
                createdAt = now
            )
        )

        // Transition to first step
        val transition = stateMachine.transition(SagaStep.STARTED, ReplyStatus.CREATED)
        advanceSaga(saga, transition, traceParent)
    }

    @Transactional
    override fun execute(sagaId: String, status: ReplyStatus, reason: String?, traceParent: String?) {
        val saga = sagaRepository.findById(sagaId)
        if (saga == null) {
            logger.warn("Saga not found", kv("saga_id", sagaId))
            return
        }

        if (saga.currentStep == SagaStep.COMPLETED || saga.currentStep == SagaStep.FAILED) {
            logger.info("Saga already in terminal state, skipping", kv("saga_id", sagaId), kv("step", saga.currentStep.name))
            return
        }

        logger.info("Processing reply", kv("saga_id", sagaId), kv("current_step", saga.currentStep.name), kv("status", status.name))

        val transition = stateMachine.transition(saga.currentStep, status)
        advanceSaga(saga, transition, traceParent, reason)
    }

    private fun advanceSaga(saga: Saga, transition: Transition, traceParent: String?, reason: String? = null) {
        val now = LocalDateTime.now()

        // Update saga state
        val updatedSaga = saga.copy(
            currentStep = transition.nextStep,
            updatedAt = now
        )
        sagaRepository.save(updatedSaga)

        // Record history
        sagaHistoryRepository.save(
            SagaHistory(
                id = UUID.randomUUID().toString(),
                sagaId = saga.id,
                step = transition.nextStep,
                status = if (transition.commandTopic != null) "PENDING" else transition.nextStep.name,
                reason = reason,
                createdAt = now
            )
        )

        logger.info("Saga advanced", kv("saga_id", saga.id), kv("new_step", transition.nextStep.name))

        // Emit command if not terminal
        if (transition.commandTopic != null && transition.commandEventType != null) {
            val outboxEvent = OutboxEvent(
                id = UUID.randomUUID().toString(),
                aggregateId = saga.id,
                aggregateType = "SAGA",
                eventType = transition.commandEventType,
                topic = transition.commandTopic,
                payload = saga.payload,
                traceParent = traceParent
            )
            outboxRepository.save(outboxEvent)
            logger.info("Command queued", kv("saga_id", saga.id), kv("topic", transition.commandTopic))
        }
    }
}

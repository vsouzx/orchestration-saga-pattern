package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.*
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.application.ports.`in`.StartSagaUseCase
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
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
    private val stateMachine: SagaStateMachine,
    private val objectMapper: ObjectMapper
) : StartSagaUseCase, HandleReplyUseCase {

    private val logger = LoggerFactory.getLogger(SagaManager::class.java)

    @Transactional
    override fun execute(orderId: String, payload: String, traceParent: String?) {
        if (sagaRepository.findByOrderId(orderId) != null) {
            logger.info("Saga already exists for order, skipping", kv("order_id", orderId))
            return
        }

        val sagaId = UUID.randomUUID().toString()
        val now = LocalDateTime.now()

        val saga = Saga(
            id = sagaId,
            orderId = orderId,
            currentStep = SagaStep.ORDER_CREATED,
            payload = payload,
            createdAt = now,
            updatedAt = now
        )
        sagaRepository.save(saga)

        logger.info("New saga created for order", kv("saga_id", sagaId), kv("order_id", orderId))

        sagaHistoryRepository.save(
            SagaHistory(
                id = UUID.randomUUID().toString(),
                sagaId = sagaId,
                step = SagaStep.ORDER_CREATED,
                createdAt = now
            )
        )

        val transition = stateMachine.transition(SagaStep.ORDER_CREATED, ReplyStatus.CREATED)
        advanceSaga(saga, transition, traceParent)
    }

    @Transactional
    override fun execute(sagaId: String, status: ReplyStatus, reason: String?, traceParent: String?) {
        val saga = sagaRepository.findById(sagaId)
        if (saga == null) {
            logger.warn("Saga not found, ignoring reply", kv("saga_id", sagaId))
            return
        }

        if (saga.currentStep == SagaStep.ORDER_COMPLETED || saga.currentStep == SagaStep.ORDER_FAILED) {
            logger.info(
                "Saga is in terminal state, ignoring reply",
                kv("saga_id", sagaId),
                kv("current_step", saga.currentStep.name)
            )
            return
        }

        val transition = stateMachine.transition(saga.currentStep, status)
        advanceSaga(saga, transition, traceParent, reason)
    }

    private fun advanceSaga(saga: Saga, transition: Transition, traceParent: String?, reason: String? = null) {
        val now = LocalDateTime.now()
        val previousStep = saga.currentStep

        // Record completedStep in history (skip if same as currentStep, e.g. ORDER_CREATED + CREATED)
        if (transition.completedStep != saga.currentStep) {
            sagaHistoryRepository.save(
                SagaHistory(
                    id = UUID.randomUUID().toString(),
                    sagaId = saga.id,
                    step = transition.completedStep,
                    reason = reason,
                    createdAt = now
                )
            )
        }

        // Record nextStep in history (skip if same as completedStep)
        if (transition.nextStep != transition.completedStep) {
            sagaHistoryRepository.save(
                SagaHistory(
                    id = UUID.randomUUID().toString(),
                    sagaId = saga.id,
                    step = transition.nextStep,
                    createdAt = now
                )
            )
        }

        // Update saga state
        val updatedSaga = saga.copy(
            currentStep = transition.nextStep,
            updatedAt = now
        )
        sagaRepository.save(updatedSaga)

        // Log the transition with descriptive message and rich structured fields
        val logArgs = mutableListOf(
            kv("saga_id", saga.id),
            kv("order_id", saga.orderId),
            kv("previous_step", previousStep.name),
            kv("completed_step", transition.completedStep.name),
            kv("next_step", transition.nextStep.name)
        )
        if (reason != null) {
            logArgs.add(kv("reason", reason))
        }
        logger.info(transition.description, *logArgs.toTypedArray())

        // Emit command if not terminal
        if (transition.commandTopic != null && transition.commandEventType != null) {
            val enrichedPayload = buildCommandPayload(updatedSaga, reason)
            val sagaWithPayload = updatedSaga.copy(payload = enrichedPayload)
            sagaRepository.save(sagaWithPayload)

            val outboxEvent = OutboxEvent(
                id = UUID.randomUUID().toString(),
                aggregateId = saga.id,
                aggregateType = "SAGA",
                eventType = transition.commandEventType,
                topic = transition.commandTopic,
                payload = enrichedPayload,
                traceParent = traceParent
            )
            outboxRepository.save(outboxEvent)

            logger.info(
                "Command queued: ${transition.commandEventType}",
                kv("saga_id", saga.id),
                kv("order_id", saga.orderId),
                kv("topic", transition.commandTopic),
                kv("event_type", transition.commandEventType)
            )
        }
    }

    private fun buildCommandPayload(saga: Saga, reason: String?): String {
        val payloadMap: MutableMap<String, Any?> = objectMapper.readValue(saga.payload)
        payloadMap["sagaId"] = saga.id
        val effectiveReason = reason ?: payloadMap["reason"] as? String
        if (effectiveReason != null) {
            payloadMap["reason"] = effectiveReason
        }
        return objectMapper.writeValueAsString(payloadMap)
    }
}

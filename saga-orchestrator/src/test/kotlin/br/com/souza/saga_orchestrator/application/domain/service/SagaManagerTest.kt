package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.*
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.kotlin.*
import org.mockito.junit.jupiter.MockitoExtension
import kotlin.test.assertEquals

@ExtendWith(MockitoExtension::class)
class SagaManagerTest {

    private val sagaRepository: SagaRepositoryPort = mock()
    private val sagaHistoryRepository: SagaHistoryRepositoryPort = mock()
    private val outboxRepository: OutboxEventRepositoryPort = mock()
    private val stateMachine = SagaStateMachine()
    private val objectMapper: ObjectMapper = jacksonObjectMapper()

    private val sagaManager = SagaManager(
        sagaRepository = sagaRepository,
        sagaHistoryRepository = sagaHistoryRepository,
        outboxRepository = outboxRepository,
        stateMachine = stateMachine,
        objectMapper = objectMapper
    )

    @Test
    fun `startSaga should create saga, record history, and emit first command`() {
        val payload = """{"orderId":"order-1","userId":"user-1","productId":1,"quantity":2,"paymentType":"PIX"}"""

        whenever(sagaRepository.findByOrderId("order-1")).thenReturn(null)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("order-1", payload, "00-traceid-spanid-01")

        // Verify saga saved: ORDER_CREATED, then RESERVING_STOCK_PENDING (state), then RESERVING_STOCK_PENDING (enriched payload)
        argumentCaptor<Saga>().apply {
            verify(sagaRepository, times(3)).save(capture())
            assertEquals(SagaStep.ORDER_CREATED, firstValue.currentStep)
            assertEquals(SagaStep.RESERVING_STOCK_PENDING, secondValue.currentStep)
            assertEquals(SagaStep.RESERVING_STOCK_PENDING, thirdValue.currentStep)
        }

        // Verify two history entries: ORDER_CREATED (initial) and RESERVING_STOCK_PENDING
        // completedStep == currentStep (ORDER_CREATED == ORDER_CREATED) so no extra completedStep entry
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository, times(2)).save(capture())
            assertEquals(SagaStep.ORDER_CREATED, firstValue.step)
            assertEquals(SagaStep.RESERVING_STOCK_PENDING, secondValue.step)
        }

        // Verify outbox event for reserve-stock command
        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("inventory.commands.reserve-stock", firstValue.topic)
            assertEquals("RESERVE_STOCK", firstValue.eventType)
            assertEquals("SAGA", firstValue.aggregateType)
            val payloadMap: Map<String, Any?> = objectMapper.readValue(firstValue.payload, Map::class.java) as Map<String, Any?>
            assertEquals(firstValue.aggregateId, payloadMap["sagaId"])
        }
    }

    @Test
    fun `startSaga should be idempotent if saga already exists`() {
        val existingSaga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.RESERVING_STOCK_PENDING,
            payload = "{}"
        )
        whenever(sagaRepository.findByOrderId("order-1")).thenReturn(existingSaga)

        sagaManager.execute("order-1", "{}", null)

        verify(sagaRepository, never()).save(any())
    }

    @Test
    fun `onReply should record completedStep and nextStep, then emit command`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.RESERVING_STOCK_PENDING,
            payload = """{"orderId":"order-1","productId":1,"quantity":2,"paymentType":"PIX","amount":5000}"""
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("saga-1", ReplyStatus.SUCCESS, null, "00-traceid-spanid-01")

        // Verify saga updated to PROCESSING_PAYMENT_PENDING
        argumentCaptor<Saga>().apply {
            verify(sagaRepository, times(2)).save(capture())
            assertEquals(SagaStep.PROCESSING_PAYMENT_PENDING, firstValue.currentStep)
            assertEquals(SagaStep.PROCESSING_PAYMENT_PENDING, secondValue.currentStep)
        }

        // Verify two history entries: RESERVING_STOCK_COMPLETED (completedStep) and PROCESSING_PAYMENT_PENDING (nextStep)
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository, times(2)).save(capture())
            assertEquals(SagaStep.RESERVING_STOCK_COMPLETED, firstValue.step)
            assertEquals(null, firstValue.reason)
            assertEquals(SagaStep.PROCESSING_PAYMENT_PENDING, secondValue.step)
        }

        // Verify outbox event for process-payment command
        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("payments.commands.process-payment", firstValue.topic)
        }
    }

    @Test
    fun `onReply should handle failure and start compensation`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.PROCESSING_PAYMENT_PENDING,
            payload = """{"orderId":"order-1","productId":1,"quantity":2,"paymentType":"BOLETO"}"""
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("saga-1", ReplyStatus.FAILURE, "BOLETO_NOT_ACCEPTED", null)

        argumentCaptor<Saga>().apply {
            verify(sagaRepository, times(2)).save(capture())
            assertEquals(SagaStep.RELEASING_STOCK_PENDING, firstValue.currentStep)
            assertEquals(SagaStep.RELEASING_STOCK_PENDING, secondValue.currentStep)
        }

        // Verify two history entries: PROCESSING_PAYMENT_FAILED (with reason) and RELEASING_STOCK_PENDING
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository, times(2)).save(capture())
            assertEquals(SagaStep.PROCESSING_PAYMENT_FAILED, firstValue.step)
            assertEquals("BOLETO_NOT_ACCEPTED", firstValue.reason)
            assertEquals(SagaStep.RELEASING_STOCK_PENDING, secondValue.step)
        }

        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("inventory.commands.release-stock", firstValue.topic)
            val payloadMap: Map<String, Any?> = objectMapper.readValue(firstValue.payload, Map::class.java) as Map<String, Any?>
            assertEquals("saga-1", payloadMap["sagaId"])
            assertEquals("BOLETO_NOT_ACCEPTED", payloadMap["reason"])
        }
    }

    @Test
    fun `onReply to terminal state should record completedStep and terminal step without outbox`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.CONFIRMING_RESERVATION_PENDING,
            payload = "{}"
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }

        sagaManager.execute("saga-1", ReplyStatus.SUCCESS, null, null)

        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.ORDER_COMPLETED, firstValue.currentStep)
        }

        // Verify two history entries: CONFIRMING_RESERVATION_COMPLETED and ORDER_COMPLETED
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository, times(2)).save(capture())
            assertEquals(SagaStep.CONFIRMING_RESERVATION_COMPLETED, firstValue.step)
            assertEquals(SagaStep.ORDER_COMPLETED, secondValue.step)
        }

        verify(outboxRepository, never()).save(any())
    }
}

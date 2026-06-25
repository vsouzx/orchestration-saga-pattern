package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.*
import br.com.souza.saga_orchestrator.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaHistoryRepositoryPort
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
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

    private val sagaManager = SagaManager(
        sagaRepository = sagaRepository,
        sagaHistoryRepository = sagaHistoryRepository,
        outboxRepository = outboxRepository,
        stateMachine = stateMachine
    )

    @Test
    fun `startSaga should create saga, record history, and emit first command`() {
        val payload = """{"orderId":"order-1","userId":"user-1","productId":1,"quantity":2,"paymentType":"PIX"}"""

        whenever(sagaRepository.findByOrderId("order-1")).thenReturn(null)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("order-1", payload, "00-traceid-spanid-01")

        // Verify saga saved twice: once as STARTED, once as RESERVING_STOCK
        argumentCaptor<Saga>().apply {
            verify(sagaRepository, times(2)).save(capture())
            assertEquals(SagaStep.STARTED, firstValue.currentStep)
            assertEquals(SagaStep.RESERVING_STOCK, secondValue.currentStep)
        }

        // Verify two history entries: STARTED and RESERVING_STOCK
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository, times(2)).save(capture())
            assertEquals(SagaStep.STARTED, firstValue.step)
            assertEquals("CREATED", firstValue.status)
            assertEquals(SagaStep.RESERVING_STOCK, secondValue.step)
            assertEquals("PENDING", secondValue.status)
        }

        // Verify outbox event for reserve-stock command
        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("inventory.commands.reserve-stock", firstValue.topic)
            assertEquals("RESERVE_STOCK", firstValue.eventType)
            assertEquals("SAGA", firstValue.aggregateType)
        }
    }

    @Test
    fun `startSaga should be idempotent if saga already exists`() {
        val existingSaga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.RESERVING_STOCK,
            payload = "{}"
        )
        whenever(sagaRepository.findByOrderId("order-1")).thenReturn(existingSaga)

        sagaManager.execute("order-1", "{}", null)

        verify(sagaRepository, never()).save(any())
    }

    @Test
    fun `onReply should advance saga and emit next command`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.RESERVING_STOCK,
            payload = """{"orderId":"order-1","productId":1,"quantity":2,"paymentType":"PIX","amount":5000}"""
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("saga-1", ReplyStatus.SUCCESS, null, "00-traceid-spanid-01")

        // Verify saga updated to PROCESSING_PAYMENT
        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.PROCESSING_PAYMENT, firstValue.currentStep)
        }

        // Verify history entry
        argumentCaptor<SagaHistory>().apply {
            verify(sagaHistoryRepository).save(capture())
            assertEquals(SagaStep.PROCESSING_PAYMENT, firstValue.step)
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
            currentStep = SagaStep.PROCESSING_PAYMENT,
            payload = """{"orderId":"order-1","productId":1,"quantity":2,"paymentType":"BOLETO"}"""
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }
        whenever(outboxRepository.save(any())).thenAnswer { it.arguments[0] as OutboxEvent }

        sagaManager.execute("saga-1", ReplyStatus.FAILURE, "BOLETO_NOT_ACCEPTED", null)

        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.RELEASING_STOCK, firstValue.currentStep)
        }

        argumentCaptor<OutboxEvent>().apply {
            verify(outboxRepository).save(capture())
            assertEquals("inventory.commands.release-stock", firstValue.topic)
        }
    }

    @Test
    fun `onReply to terminal state should not emit outbox event`() {
        val saga = Saga(
            id = "saga-1",
            orderId = "order-1",
            currentStep = SagaStep.CONFIRMING_RESERVATION,
            payload = "{}"
        )
        whenever(sagaRepository.findById("saga-1")).thenReturn(saga)
        whenever(sagaRepository.save(any())).thenAnswer { it.arguments[0] as Saga }
        whenever(sagaHistoryRepository.save(any())).thenAnswer { it.arguments[0] as SagaHistory }

        sagaManager.execute("saga-1", ReplyStatus.SUCCESS, null, null)

        argumentCaptor<Saga>().apply {
            verify(sagaRepository).save(capture())
            assertEquals(SagaStep.COMPLETED, firstValue.currentStep)
        }

        verify(outboxRepository, never()).save(any())
    }
}

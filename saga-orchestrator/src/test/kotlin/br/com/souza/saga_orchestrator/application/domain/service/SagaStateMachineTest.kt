package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class SagaStateMachineTest {

    private val stateMachine = SagaStateMachine()

    // Happy path transitions
    @Test
    fun `ORDER_CREATED + CREATED should transition to RESERVING_STOCK_PENDING`() {
        val transition = stateMachine.transition(SagaStep.ORDER_CREATED, ReplyStatus.CREATED)
        assertEquals(SagaStep.ORDER_CREATED, transition.completedStep)
        assertEquals(SagaStep.RESERVING_STOCK_PENDING, transition.nextStep)
        assertEquals("inventory.commands.reserve-stock", transition.commandTopic)
        assertEquals("RESERVE_STOCK", transition.commandEventType)
        assertEquals("Order created, advancing to reserve stock", transition.description)
    }

    @Test
    fun `RESERVING_STOCK_PENDING + SUCCESS should transition to PROCESSING_PAYMENT_PENDING`() {
        val transition = stateMachine.transition(SagaStep.RESERVING_STOCK_PENDING, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.RESERVING_STOCK_COMPLETED, transition.completedStep)
        assertEquals(SagaStep.PROCESSING_PAYMENT_PENDING, transition.nextStep)
        assertEquals("payments.commands.process-payment", transition.commandTopic)
        assertEquals("PROCESS_PAYMENT", transition.commandEventType)
        assertEquals("Stock reserved successfully, advancing to process payment", transition.description)
    }

    @Test
    fun `PROCESSING_PAYMENT_PENDING + SUCCESS should transition to CONFIRMING_ORDER_PENDING`() {
        val transition = stateMachine.transition(SagaStep.PROCESSING_PAYMENT_PENDING, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.PROCESSING_PAYMENT_COMPLETED, transition.completedStep)
        assertEquals(SagaStep.CONFIRMING_ORDER_PENDING, transition.nextStep)
        assertEquals("orders.commands.confirm-order", transition.commandTopic)
        assertEquals("CONFIRM_ORDER", transition.commandEventType)
        assertEquals("Payment processed successfully, advancing to confirm order", transition.description)
    }

    @Test
    fun `CONFIRMING_ORDER_PENDING + SUCCESS should transition to CONFIRMING_RESERVATION_PENDING`() {
        val transition = stateMachine.transition(SagaStep.CONFIRMING_ORDER_PENDING, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CONFIRMING_ORDER_COMPLETED, transition.completedStep)
        assertEquals(SagaStep.CONFIRMING_RESERVATION_PENDING, transition.nextStep)
        assertEquals("inventory.commands.confirm-reservation", transition.commandTopic)
        assertEquals("CONFIRM_RESERVATION", transition.commandEventType)
        assertEquals("Order confirmed, advancing to confirm reservation", transition.description)
    }

    @Test
    fun `CONFIRMING_RESERVATION_PENDING + SUCCESS should transition to ORDER_COMPLETED`() {
        val transition = stateMachine.transition(SagaStep.CONFIRMING_RESERVATION_PENDING, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CONFIRMING_RESERVATION_COMPLETED, transition.completedStep)
        assertEquals(SagaStep.ORDER_COMPLETED, transition.nextStep)
        assertEquals(null, transition.commandTopic)
        assertEquals(null, transition.commandEventType)
        assertEquals("Reservation confirmed, saga completed successfully", transition.description)
    }

    // Compensation transitions
    @Test
    fun `RESERVING_STOCK_PENDING + FAILURE should transition to CANCELING_ORDER_PENDING`() {
        val transition = stateMachine.transition(SagaStep.RESERVING_STOCK_PENDING, ReplyStatus.FAILURE)
        assertEquals(SagaStep.RESERVING_STOCK_FAILED, transition.completedStep)
        assertEquals(SagaStep.CANCELING_ORDER_PENDING, transition.nextStep)
        assertEquals("orders.commands.cancel-order", transition.commandTopic)
        assertEquals("CANCEL_ORDER", transition.commandEventType)
        assertEquals("Stock reservation failed, compensating — canceling order", transition.description)
    }

    @Test
    fun `PROCESSING_PAYMENT_PENDING + FAILURE should transition to RELEASING_STOCK_PENDING`() {
        val transition = stateMachine.transition(SagaStep.PROCESSING_PAYMENT_PENDING, ReplyStatus.FAILURE)
        assertEquals(SagaStep.PROCESSING_PAYMENT_FAILED, transition.completedStep)
        assertEquals(SagaStep.RELEASING_STOCK_PENDING, transition.nextStep)
        assertEquals("inventory.commands.release-stock", transition.commandTopic)
        assertEquals("RELEASE_STOCK", transition.commandEventType)
        assertEquals("Payment failed, compensating — releasing stock", transition.description)
    }

    @Test
    fun `RELEASING_STOCK_PENDING + SUCCESS should transition to CANCELING_ORDER_PENDING`() {
        val transition = stateMachine.transition(SagaStep.RELEASING_STOCK_PENDING, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.RELEASING_STOCK_COMPLETED, transition.completedStep)
        assertEquals(SagaStep.CANCELING_ORDER_PENDING, transition.nextStep)
        assertEquals("orders.commands.cancel-order", transition.commandTopic)
        assertEquals("CANCEL_ORDER", transition.commandEventType)
        assertEquals("Stock released, continuing compensation — canceling order", transition.description)
    }

    @Test
    fun `CANCELING_ORDER_PENDING + SUCCESS should transition to ORDER_FAILED`() {
        val transition = stateMachine.transition(SagaStep.CANCELING_ORDER_PENDING, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CANCELING_ORDER_COMPLETED, transition.completedStep)
        assertEquals(SagaStep.ORDER_FAILED, transition.nextStep)
        assertEquals(null, transition.commandTopic)
        assertEquals(null, transition.commandEventType)
        assertEquals("Order canceled, saga failed", transition.description)
    }

    // Terminal states
    @Test
    fun `ORDER_COMPLETED is terminal and should throw`() {
        assertThrows<IllegalStateException> {
            stateMachine.transition(SagaStep.ORDER_COMPLETED, ReplyStatus.SUCCESS)
        }
    }

    @Test
    fun `ORDER_FAILED is terminal and should throw`() {
        assertThrows<IllegalStateException> {
            stateMachine.transition(SagaStep.ORDER_FAILED, ReplyStatus.SUCCESS)
        }
    }
}

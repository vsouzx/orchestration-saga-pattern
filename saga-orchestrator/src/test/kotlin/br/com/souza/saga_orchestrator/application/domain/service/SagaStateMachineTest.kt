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
    fun `STARTED + SUCCESS should transition to RESERVING_STOCK`() {
        val transition = stateMachine.transition(SagaStep.STARTED, ReplyStatus.CREATED)
        assertEquals(SagaStep.RESERVING_STOCK, transition.nextStep)
        assertEquals("inventory.commands.reserve-stock", transition.commandTopic)
        assertEquals("RESERVE_STOCK", transition.commandEventType)
    }

    @Test
    fun `RESERVING_STOCK + SUCCESS should transition to PROCESSING_PAYMENT`() {
        val transition = stateMachine.transition(SagaStep.RESERVING_STOCK, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.PROCESSING_PAYMENT, transition.nextStep)
        assertEquals("payments.commands.process-payment", transition.commandTopic)
        assertEquals("PROCESS_PAYMENT", transition.commandEventType)
    }

    @Test
    fun `PROCESSING_PAYMENT + SUCCESS should transition to CONFIRMING_ORDER`() {
        val transition = stateMachine.transition(SagaStep.PROCESSING_PAYMENT, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CONFIRMING_ORDER, transition.nextStep)
        assertEquals("orders.commands.confirm-order", transition.commandTopic)
        assertEquals("CONFIRM_ORDER", transition.commandEventType)
    }

    @Test
    fun `CONFIRMING_ORDER + SUCCESS should transition to CONFIRMING_RESERVATION`() {
        val transition = stateMachine.transition(SagaStep.CONFIRMING_ORDER, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CONFIRMING_RESERVATION, transition.nextStep)
        assertEquals("inventory.commands.confirm-reservation", transition.commandTopic)
        assertEquals("CONFIRM_RESERVATION", transition.commandEventType)
    }

    @Test
    fun `CONFIRMING_RESERVATION + SUCCESS should transition to COMPLETED`() {
        val transition = stateMachine.transition(SagaStep.CONFIRMING_RESERVATION, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.COMPLETED, transition.nextStep)
        assertEquals(null, transition.commandTopic)
    }

    // Compensation transitions
    @Test
    fun `RESERVING_STOCK + FAILURE should transition to CANCELING_ORDER`() {
        val transition = stateMachine.transition(SagaStep.RESERVING_STOCK, ReplyStatus.FAILURE)
        assertEquals(SagaStep.CANCELING_ORDER, transition.nextStep)
        assertEquals("orders.commands.cancel-order", transition.commandTopic)
        assertEquals("CANCEL_ORDER", transition.commandEventType)
    }

    @Test
    fun `PROCESSING_PAYMENT + FAILURE should transition to RELEASING_STOCK`() {
        val transition = stateMachine.transition(SagaStep.PROCESSING_PAYMENT, ReplyStatus.FAILURE)
        assertEquals(SagaStep.RELEASING_STOCK, transition.nextStep)
        assertEquals("inventory.commands.release-stock", transition.commandTopic)
        assertEquals("RELEASE_STOCK", transition.commandEventType)
    }

    @Test
    fun `RELEASING_STOCK + SUCCESS should transition to CANCELING_ORDER`() {
        val transition = stateMachine.transition(SagaStep.RELEASING_STOCK, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.CANCELING_ORDER, transition.nextStep)
        assertEquals("orders.commands.cancel-order", transition.commandTopic)
        assertEquals("CANCEL_ORDER", transition.commandEventType)
    }

    @Test
    fun `CANCELING_ORDER + SUCCESS should transition to FAILED`() {
        val transition = stateMachine.transition(SagaStep.CANCELING_ORDER, ReplyStatus.SUCCESS)
        assertEquals(SagaStep.FAILED, transition.nextStep)
        assertEquals(null, transition.commandTopic)
    }

    // Terminal states
    @Test
    fun `COMPLETED is terminal and should throw`() {
        assertThrows<IllegalStateException> {
            stateMachine.transition(SagaStep.COMPLETED, ReplyStatus.SUCCESS)
        }
    }

    @Test
    fun `FAILED is terminal and should throw`() {
        assertThrows<IllegalStateException> {
            stateMachine.transition(SagaStep.FAILED, ReplyStatus.SUCCESS)
        }
    }
}

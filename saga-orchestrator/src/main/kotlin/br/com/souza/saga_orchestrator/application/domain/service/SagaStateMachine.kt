package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

data class Transition(
    val nextStep: SagaStep,
    val commandTopic: String?,
    val commandEventType: String?
)

class SagaStateMachine {

    private data class TransitionKey(val step: SagaStep, val status: ReplyStatus)

    private val transitions = mapOf(
        // Happy path
        TransitionKey(SagaStep.STARTED, ReplyStatus.CREATED) to Transition(
            SagaStep.RESERVING_STOCK, "inventory.commands.reserve-stock", "RESERVE_STOCK"
        ),
        TransitionKey(SagaStep.RESERVING_STOCK, ReplyStatus.SUCCESS) to Transition(
            SagaStep.PROCESSING_PAYMENT, "payments.commands.process-payment", "PROCESS_PAYMENT"
        ),
        TransitionKey(SagaStep.PROCESSING_PAYMENT, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CONFIRMING_ORDER, "orders.commands.confirm-order", "CONFIRM_ORDER"
        ),
        TransitionKey(SagaStep.CONFIRMING_ORDER, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CONFIRMING_RESERVATION, "inventory.commands.confirm-reservation", "CONFIRM_RESERVATION"
        ),
        TransitionKey(SagaStep.CONFIRMING_RESERVATION, ReplyStatus.SUCCESS) to Transition(
            SagaStep.COMPLETED, null, null
        ),

        // Compensation
        TransitionKey(SagaStep.RESERVING_STOCK, ReplyStatus.FAILURE) to Transition(
            SagaStep.CANCELING_ORDER, "orders.commands.cancel-order", "CANCEL_ORDER"
        ),
        TransitionKey(SagaStep.PROCESSING_PAYMENT, ReplyStatus.FAILURE) to Transition(
            SagaStep.RELEASING_STOCK, "inventory.commands.release-stock", "RELEASE_STOCK"
        ),
        TransitionKey(SagaStep.RELEASING_STOCK, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CANCELING_ORDER, "orders.commands.cancel-order", "CANCEL_ORDER"
        ),
        TransitionKey(SagaStep.CANCELING_ORDER, ReplyStatus.SUCCESS) to Transition(
            SagaStep.FAILED, null, null
        )
    )

    fun transition(currentStep: SagaStep, replyStatus: ReplyStatus): Transition {
        if (currentStep == SagaStep.COMPLETED || currentStep == SagaStep.FAILED) {
            throw IllegalStateException("Cannot transition from terminal state: $currentStep")
        }

        return transitions[TransitionKey(currentStep, replyStatus)]
            ?: throw IllegalStateException("No transition defined for step=$currentStep, status=$replyStatus")
    }
}

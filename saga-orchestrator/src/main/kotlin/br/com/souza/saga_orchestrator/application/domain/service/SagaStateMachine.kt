package br.com.souza.saga_orchestrator.application.domain.service

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep
import org.springframework.stereotype.Component

data class Transition(
    val completedStep: SagaStep,
    val nextStep: SagaStep,
    val commandTopic: String?,
    val commandEventType: String?,
    val description: String
)

@Component
class SagaStateMachine {

    private data class TransitionKey(val step: SagaStep, val status: ReplyStatus)

    private val transitions = mapOf(
        // Happy path
        TransitionKey(SagaStep.ORDER_CREATED, ReplyStatus.CREATED) to Transition(
            SagaStep.ORDER_CREATED, SagaStep.RESERVING_STOCK_PENDING,
            "inventory.commands.reserve-stock", "RESERVE_STOCK",
            "Order created, advancing to reserve stock"
        ),
        TransitionKey(SagaStep.RESERVING_STOCK_PENDING, ReplyStatus.SUCCESS) to Transition(
            SagaStep.RESERVING_STOCK_COMPLETED, SagaStep.PROCESSING_PAYMENT_PENDING,
            "payments.commands.process-payment", "PROCESS_PAYMENT",
            "Stock reserved successfully, advancing to process payment"
        ),
        TransitionKey(SagaStep.PROCESSING_PAYMENT_PENDING, ReplyStatus.SUCCESS) to Transition(
            SagaStep.PROCESSING_PAYMENT_COMPLETED, SagaStep.CONFIRMING_ORDER_PENDING,
            "orders.commands.confirm-order", "CONFIRM_ORDER",
            "Payment processed successfully, advancing to confirm order"
        ),
        TransitionKey(SagaStep.CONFIRMING_ORDER_PENDING, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CONFIRMING_ORDER_COMPLETED, SagaStep.CONFIRMING_RESERVATION_PENDING,
            "inventory.commands.confirm-reservation", "CONFIRM_RESERVATION",
            "Order confirmed, advancing to confirm reservation"
        ),
        TransitionKey(SagaStep.CONFIRMING_RESERVATION_PENDING, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CONFIRMING_RESERVATION_COMPLETED, SagaStep.ORDER_COMPLETED,
            null, null,
            "Reservation confirmed, saga completed successfully"
        ),

        // Compensation
        TransitionKey(SagaStep.RESERVING_STOCK_PENDING, ReplyStatus.FAILURE) to Transition(
            SagaStep.RESERVING_STOCK_FAILED, SagaStep.CANCELING_ORDER_PENDING,
            "orders.commands.cancel-order", "CANCEL_ORDER",
            "Stock reservation failed, compensating — canceling order"
        ),
        TransitionKey(SagaStep.PROCESSING_PAYMENT_PENDING, ReplyStatus.FAILURE) to Transition(
            SagaStep.PROCESSING_PAYMENT_FAILED, SagaStep.RELEASING_STOCK_PENDING,
            "inventory.commands.release-stock", "RELEASE_STOCK",
            "Payment failed, compensating — releasing stock"
        ),
        TransitionKey(SagaStep.RELEASING_STOCK_PENDING, ReplyStatus.SUCCESS) to Transition(
            SagaStep.RELEASING_STOCK_COMPLETED, SagaStep.CANCELING_ORDER_PENDING,
            "orders.commands.cancel-order", "CANCEL_ORDER",
            "Stock released, continuing compensation — canceling order"
        ),
        TransitionKey(SagaStep.CANCELING_ORDER_PENDING, ReplyStatus.SUCCESS) to Transition(
            SagaStep.CANCELING_ORDER_COMPLETED, SagaStep.ORDER_FAILED,
            null, null,
            "Order canceled, saga failed"
        )
    )

    fun transition(currentStep: SagaStep, replyStatus: ReplyStatus): Transition {
        if (currentStep == SagaStep.ORDER_COMPLETED || currentStep == SagaStep.ORDER_FAILED) {
            throw IllegalStateException("Cannot transition from terminal state: $currentStep")
        }

        return transitions[TransitionKey(currentStep, replyStatus)]
            ?: throw IllegalStateException("No transition defined for step=$currentStep, status=$replyStatus")
    }
}

package br.com.souza.saga_orchestrator.application.ports.`in`

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep

interface HandleReplyUseCase {
    fun execute(sagaId: String, expectedStep: SagaStep, status: ReplyStatus, reason: String?, traceParent: String?)
}

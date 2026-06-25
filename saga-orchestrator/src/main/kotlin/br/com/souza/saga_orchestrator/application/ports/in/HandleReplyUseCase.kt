package br.com.souza.saga_orchestrator.application.ports.`in`

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus

interface HandleReplyUseCase {
    fun execute(sagaId: String, status: ReplyStatus, reason: String?, traceParent: String?)
}

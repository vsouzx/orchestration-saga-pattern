package br.com.souza.saga_orchestrator.application.ports.`in`

interface StartSagaUseCase {
    fun execute(orderId: String, payload: String, traceParent: String?)
}

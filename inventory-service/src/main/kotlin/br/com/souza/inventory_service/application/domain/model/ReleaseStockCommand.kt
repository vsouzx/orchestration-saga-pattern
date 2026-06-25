package br.com.souza.inventory_service.application.domain.model

data class ReleaseStockCommand(
    val sagaId: String,
    val orderId: String,
    val traceParent: String?
)

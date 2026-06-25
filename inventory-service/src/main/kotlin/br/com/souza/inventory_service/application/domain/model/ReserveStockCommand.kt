package br.com.souza.inventory_service.application.domain.model

data class ReserveStockCommand(
    val sagaId: String,
    val orderId: String,
    val productId: Int,
    val quantity: Int,
    val paymentType: String,
    val traceParent: String?
)

package br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class SagaCommand(
    val sagaId: String = "",
    val orderId: String = "",
    val productId: Int = 0,
    val quantity: Int = 0,
    val paymentType: String = ""
)

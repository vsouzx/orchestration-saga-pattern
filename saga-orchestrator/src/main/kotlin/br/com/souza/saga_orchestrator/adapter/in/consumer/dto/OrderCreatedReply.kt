package br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class OrderCreatedReply(
    val orderId: String = "",
    val userId: String = "",
    val productId: Int = 0,
    val quantity: Int = 0,
    val paymentType: String = "",
    val status: String = ""
)

package br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class SagaReplyEvent(
    val sagaId: String = "",
    val orderId: String = "",
    val status: String = "",
    val reason: String? = null
)

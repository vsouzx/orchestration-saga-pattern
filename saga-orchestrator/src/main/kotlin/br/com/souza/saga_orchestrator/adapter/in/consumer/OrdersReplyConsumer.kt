package br.com.souza.saga_orchestrator.adapter.`in`.consumer

import br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto.OrderCreatedReply
import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.application.ports.`in`.StartSagaUseCase
import br.com.souza.saga_orchestrator.infrastructure.observability.TraceContextExtractor
import com.fasterxml.jackson.databind.ObjectMapper
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class OrdersReplyConsumer(
    private val startSaga: StartSagaUseCase,
    private val handleReply: HandleReplyUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(OrdersReplyConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("saga-orchestrator")
    private val objectMapper = ObjectMapper()

    @KafkaListener(
        topics = ["orders.replies"],
        groupId = "saga-orchestrator",
        containerFactory = "ordersReplyKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload reply: OrderCreatedReply,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("orders.replies process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                if (reply.status == "CREATED") {
                    logger.info(
                        "Received order creation reply, starting saga",
                        kv("order_id", reply.orderId),
                        kv("reply_status", reply.status)
                    )
                    val payload = objectMapper.writeValueAsString(reply)
                    startSaga.execute(reply.orderId, payload, traceParent)
                } else {
                    logger.info(
                        "Received order reply for saga",
                        kv("saga_id", reply.sagaId),
                        kv("order_id", reply.orderId),
                        kv("reply_status", reply.status)
                    )
                    handleReply.execute(reply.sagaId, ReplyStatus.valueOf(reply.status), reply.reason, traceParent)
                }
            } finally {
                span.end()
            }
        }
    }
}

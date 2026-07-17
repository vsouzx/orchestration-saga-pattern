package br.com.souza.saga_orchestrator.adapter.`in`.consumer

import br.com.souza.saga_orchestrator.adapter.`in`.consumer.dto.SagaReplyEvent
import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.domain.model.SagaStep
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ReleaseStockReplyConsumer(
    private val handleReply: HandleReplyUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(ReleaseStockReplyConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("saga-orchestrator")

    @KafkaListener(
        topics = ["inventory.replies.release-stock"],
        groupId = "saga-orchestrator",
        containerFactory = "sagaReplyKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload reply: SagaReplyEvent,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.replies.release-stock process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received release-stock reply", kv("saga_id", reply.sagaId), kv("reply_status", reply.status))
                handleReply.execute(reply.sagaId, SagaStep.RELEASING_STOCK_PENDING, ReplyStatus.valueOf(reply.status), reply.reason, traceParent)
            } finally {
                span.end()
            }
        }
    }
}

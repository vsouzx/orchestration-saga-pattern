package br.com.souza.inventory_service.adapter.`in`.consumer.saga

import br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto.SagaCommand
import br.com.souza.inventory_service.application.domain.model.ReleaseStockCommand
import br.com.souza.inventory_service.application.ports.`in`.ReleaseStockUseCase
import br.com.souza.inventory_service.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ReleaseStockCommandConsumer(
    private val releaseStock: ReleaseStockUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(ReleaseStockCommandConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(
        topics = ["inventory.commands.release-stock"],
        groupId = "inventory-service",
        containerFactory = "sagaCommandKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload command: SagaCommand,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.commands.release-stock process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received release-stock command", kv("saga_id", command.sagaId), kv("order_id", command.orderId))

                val cmd = ReleaseStockCommand(
                    sagaId = command.sagaId,
                    orderId = command.orderId,
                    traceParent = traceParent
                )

                releaseStock.execute(cmd)
            } finally {
                span.end()
            }
        }
    }
}

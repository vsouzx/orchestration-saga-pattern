package br.com.souza.inventory_service.adapter.`in`.consumer.saga

import br.com.souza.inventory_service.adapter.`in`.consumer.saga.dto.SagaCommand
import br.com.souza.inventory_service.application.domain.model.ReserveStockCommand
import br.com.souza.inventory_service.application.ports.`in`.ReserveStockUseCase
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
class ReserveStockCommandConsumer(
    private val reserveStock: ReserveStockUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(ReserveStockCommandConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(
        topics = ["inventory.commands.reserve-stock"],
        groupId = "inventory-service",
        containerFactory = "sagaCommandKafkaListenerContainerFactory"
    )
    fun consume(
        @Payload command: SagaCommand,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("inventory.commands.reserve-stock process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received reserve-stock command", kv("saga_id", command.sagaId), kv("order_id", command.orderId))

                val cmd = ReserveStockCommand(
                    sagaId = command.sagaId,
                    orderId = command.orderId,
                    productId = command.productId,
                    quantity = command.quantity,
                    paymentType = command.paymentType,
                    traceParent = traceParent
                )

                reserveStock.execute(cmd)
            } finally {
                span.end()
            }
        }
    }
}

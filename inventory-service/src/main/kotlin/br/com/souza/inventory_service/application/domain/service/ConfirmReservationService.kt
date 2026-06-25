package br.com.souza.inventory_service.application.domain.service

import br.com.souza.inventory_service.application.domain.model.ConfirmReservationCommand
import br.com.souza.inventory_service.application.domain.model.OutboxEvent
import br.com.souza.inventory_service.application.domain.model.ReservationStatus
import br.com.souza.inventory_service.application.ports.`in`.ConfirmReservationUseCase
import br.com.souza.inventory_service.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.inventory_service.application.ports.out.StockReservationRepositoryPort
import com.fasterxml.jackson.databind.ObjectMapper
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class ConfirmReservationService(
    private val reservationRepository: StockReservationRepositoryPort,
    private val outboxRepository: OutboxEventRepositoryPort
) : ConfirmReservationUseCase {

    private val logger = LoggerFactory.getLogger(ConfirmReservationService::class.java)
    private val objectMapper = ObjectMapper()
    private val aggregateType = "ORDER"
    private val eventType = "INVENTORY_REPLY"

    @Transactional
    override fun execute(command: ConfirmReservationCommand) {
        logger.info("Processing reservation confirmation", kv("saga_id", command.sagaId), kv("order_id", command.orderId))

        val reservation = reservationRepository.findByOrderId(command.orderId)
        if (reservation == null) {
            logger.warn("Reservation not found, skipping confirmation", kv("order_id", command.orderId))
            return
        }

        val updatedReservation = reservation.copy(status = ReservationStatus.CONFIRMED)
        reservationRepository.save(updatedReservation)
        logger.info("Reservation confirmed", kv("order_id", command.orderId), kv("reservation_id", reservation.id))

        val payload = objectMapper.writeValueAsString(
            mapOf(
                "sagaId" to command.sagaId,
                "orderId" to command.orderId,
                "status" to "SUCCESS"
            )
        )

        val outboxEvent = OutboxEvent(
            id = UUID.randomUUID().toString(),
            aggregateId = command.orderId,
            aggregateType = aggregateType,
            eventType = eventType,
            topic = "inventory.replies",
            payload = payload,
            traceParent = command.traceParent
        )
        outboxRepository.save(outboxEvent)
        logger.info("Outbox event saved", kv("topic", "inventory.replies"), kv("saga_id", command.sagaId), kv("order_id", command.orderId), kv("status", "SUCCESS"))
    }
}

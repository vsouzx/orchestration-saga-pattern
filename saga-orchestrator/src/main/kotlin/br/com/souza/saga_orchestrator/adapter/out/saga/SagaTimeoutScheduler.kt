package br.com.souza.saga_orchestrator.adapter.out.saga

import br.com.souza.saga_orchestrator.application.domain.model.ReplyStatus
import br.com.souza.saga_orchestrator.application.ports.`in`.HandleReplyUseCase
import br.com.souza.saga_orchestrator.application.ports.out.SagaRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class SagaTimeoutScheduler(
    private val sagaRepository: SagaRepositoryPort,
    private val handleReply: HandleReplyUseCase,
    @Value("\${saga.timeout-minutes:5}") private val timeoutMinutes: Long
) {

    private val logger = LoggerFactory.getLogger(SagaTimeoutScheduler::class.java)

    @Scheduled(fixedDelay = 60000)
    fun checkTimeouts() {
        val cutoff = LocalDateTime.now().minusMinutes(timeoutMinutes)
        val timedOut = sagaRepository.findTimedOutSagas(cutoff)

        for (saga in timedOut) {
            logger.warn("Saga timed out, starting compensation",
                kv("saga_id", saga.id),
                kv("order_id", saga.orderId),
                kv("current_step", saga.currentStep.name)
            )
            try {
                handleReply.execute(saga.id, ReplyStatus.FAILURE, "TIMEOUT", null)
            } catch (ex: Exception) {
                logger.error("Error compensating timed out saga", kv("saga_id", saga.id), ex)
            }
        }
    }
}

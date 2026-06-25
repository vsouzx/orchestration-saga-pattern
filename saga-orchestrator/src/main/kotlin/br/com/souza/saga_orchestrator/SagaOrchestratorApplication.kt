package br.com.souza.saga_orchestrator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class SagaOrchestratorApplication

fun main(args: Array<String>) {
    runApplication<SagaOrchestratorApplication>(*args)
}

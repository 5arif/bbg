package com.desmo.bbg.listener

import com.desmo.bbg.model.ServiceEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class  ServiceEventListener {
    private val log = LoggerFactory.getLogger(OrderEventListener::class.java)
    private val mapper = ObjectMapper().registerKotlinModule()

    @KafkaListener(topics = ["services.events"], containerFactory = "kafkaListenerContainerFactory")
    fun onMessage(@Payload event: ServiceEvent, record: ConsumerRecord<String, Any>, ack: Acknowledgment) {
        try {
            // Business logic here
            processEvent(event)

            // Commit offset after successful processing
            ack.acknowledge()
        } catch (ex: Exception) {
            log.error("Error processing record at offset ${record.offset()}", ex)
            // Don't ack; DefaultErrorHandler will manage retries / DLT if configured.
            throw ex
        }
    }

    private fun processEvent(event: ServiceEvent) {
        // Example processing
        require(event.description != null || event.description != "") { "Please specify description" }

        val eventStr = mapper.writeValueAsString(event)
        log.info(eventStr)
        // ...persist, call other services, etc.
    }
}
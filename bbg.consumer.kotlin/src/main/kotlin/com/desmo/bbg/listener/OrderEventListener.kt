package com.desmo.bbg.listener

import com.desmo.bbg.model.OrderEvent
import tools.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class OrderEventListener(private val mapper: ObjectMapper) {

    private val log = LoggerFactory.getLogger(OrderEventListener::class.java)

    @KafkaListener(
        topics = ["orders.events"],
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onMessage(@Payload event: OrderEvent, record: ConsumerRecord<String, Any>, ack: Acknowledgment) {
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

    private fun processEvent(event: OrderEvent) {
        // Example processing
        require(event.amount >= 0) { "Amount cannot be negative" }

        val eventStr = mapper.writeValueAsString(event)
        log.info(eventStr)
        // ...persist, call other services, etc.
    }
}


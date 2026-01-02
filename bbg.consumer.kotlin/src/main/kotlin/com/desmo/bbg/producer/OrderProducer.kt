package com.desmo.bbg.producer

import com.desmo.bbg.model.OrderEvent
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets

@Service
class OrderProducer(
    private val kafkaTemplate: KafkaTemplate<String, OrderEvent>,
    @Value("\${app.kafka.topic}") private val topic: String
) {

    fun send(key: String, event: OrderEvent) {
        val record = ProducerRecord(topic, null, System.currentTimeMillis(), key, event).apply {
            headers().add(
                RecordHeader("event-source", "order-service".toByteArray(StandardCharsets.UTF_8))
            )
        }

        val future = kafkaTemplate.send(record)
        future.whenComplete { result, throwable ->
            if (throwable != null) {
                System.err.println("Send failed: ${throwable.message}")
            } else {
                val md = result.recordMetadata
                println("Sent OK: topic=${md.topic()} partition=${md.partition()} offset=${md.offset()}")
            }
        }
    }
}

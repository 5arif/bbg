
package com.desmo.bbg.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries

@Configuration
class KafkaConsumerConfig(
    @org.springframework.beans.factory.annotation.Value("\${spring.kafka.bootstrap-servers:localhost:9092}")
    private val bootstrapServers: String
) {

    // --- Consumer side ---
    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "order-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java, // Changed to StringDeserializer
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Any>,
        defaultErrorHandler: DefaultErrorHandler
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.setConsumerFactory(consumerFactory)
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.setCommonErrorHandler(defaultErrorHandler)
        factory.setConcurrency(3)
        // Add Message Converter to handle JSON conversion based on target type
        factory.setRecordMessageConverter(org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter())
        return factory
    }

    // --- Producer side (needed for DLT publisher) ---
    @Bean
    fun producerFactory(): ProducerFactory<String, Any> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to org.springframework.kafka.support.serializer.JacksonJsonSerializer::class.java,
            // Optional: include type info in headers
            "spring.json.add.type.headers" to true
        )
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Any> =
        KafkaTemplate(producerFactory())

    @Bean
    fun deadLetterPublishingRecoverer(template: KafkaTemplate<String, Any>) =
        DeadLetterPublishingRecoverer(template) { record, _ ->
            // Send DLT to <topic>.DLT, same partition
            org.apache.kafka.common.TopicPartition("${record.topic()}.DLT", record.partition())
        }

    @Bean
    fun defaultErrorHandler(dlt: DeadLetterPublishingRecoverer): DefaultErrorHandler {
        val backoff = ExponentialBackOffWithMaxRetries(3).apply {
            initialInterval = 500
            multiplier = 2.0
            maxInterval = 5000
        }
        return DefaultErrorHandler(dlt, backoff)
    }
}

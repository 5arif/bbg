package com.desmo.bbg.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer

@Configuration
class KafkaConsumerConfig {

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ConsumerConfig.GROUP_ID_CONFIG to "sample-consumer-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JacksonJsonDeserializer::class.java,

            // Trust packages for JSON (you may restrict this to your packages)
            "spring.json.trusted.packages" to "*",
            "spring.json.use.type.headers" to "true",
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
        factory.setConcurrency(3) // aligns with spring.kafka.listener.concurrency

        return factory
    }

    @Bean
    fun defaultErrorHandler(): DefaultErrorHandler {
        // Exponential backoff retry: 3 attempts, starting 500ms, multiplier 2.0, max 5s
        val backoff = ExponentialBackOffWithMaxRetries(3).apply {
            initialInterval = 500
            multiplier = 2.0
            maxInterval = 5000
        }
        return DefaultErrorHandler(backoff).apply {
            // Example: classify exceptions as non-retryable
            // addNotRetryableExceptions(IllegalArgumentException::class.java)
        }
    }

    @Bean
    fun deadLetterPublishingRecoverer(template: KafkaTemplate<String, Any>) =
        DeadLetterPublishingRecoverer(template) { record, _ ->
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

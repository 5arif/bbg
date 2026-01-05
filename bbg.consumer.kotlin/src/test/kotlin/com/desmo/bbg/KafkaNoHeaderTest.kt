package com.desmo.bbg

import com.desmo.bbg.consumer.OrderEventListener
import com.desmo.bbg.model.OrderEvent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
// import org.springframework.boot.test.mock.mockito.SpyBean // Deprecated/Removed
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean 
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.TestPropertySource
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka(
    topics = ["orders.events"],
    partitions = 1
)
@TestPropertySource(properties = [
    "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}", // Use correct property name
    "spring.kafka.consumer.bootstrap-servers=\${spring.embedded.kafka.brokers}",
    "spring.kafka.producer.bootstrap-servers=\${spring.embedded.kafka.brokers}"
])
class KafkaNoHeaderTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, Any>

    @MockitoSpyBean
    private lateinit var orderEventListener: OrderEventListener

    @Test
    fun `should consume message without type headers`() {
        // Send a raw JSON string without any type headers
        // Using a plain String here mimics a producer that doesn't add type info
        val json = """{"id":"123", "amount":100.0}"""
        
        kafkaTemplate.send("orders.events", "key-1", json).get(10, TimeUnit.SECONDS)

        // Verify that the listener received the converted object
        val captor = ArgumentCaptor.forClass(OrderEvent::class.java)
        
        // Wait for the message to be consumed (mockito verify with timeout)
        verify(orderEventListener, Mockito.timeout(10000).times(1)).onMessage(captor.capture(), Mockito.any(), Mockito.any())
        
        val event = captor.value
        assertEquals("123", event.orderId)
        assertEquals(100.0, event.amount)
    }
}

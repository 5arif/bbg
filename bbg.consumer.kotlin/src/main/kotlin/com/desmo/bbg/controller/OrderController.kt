package com.desmo.bbg.controller

import com.desmo.bbg.model.OrderEvent
import com.desmo.bbg.producer.OrderProducer
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val producer: OrderProducer
) {
    @PostMapping("/{orderId}")
    fun create(
        @PathVariable orderId: String,
        @RequestBody payload: OrderEvent
    ): ResponseEntity<String> {
        producer.send(orderId, payload)
        return ResponseEntity.accepted().body("Queued order $orderId")
    }
}

package com.desmo.bbg.model

data class OrderEvent(
    val orderId: String,
    val amount: Double,
    val customer: String? = null
)

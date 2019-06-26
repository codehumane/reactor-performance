package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class Step1ItemGenerator {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelayMillis(source: StartItem, maxDelayInMillis: Int): Step1Item {
        Thread.sleep(random.nextInt(maxDelayInMillis).toLong())
        return Step1Item(source, id.getAndIncrement())
    }

}
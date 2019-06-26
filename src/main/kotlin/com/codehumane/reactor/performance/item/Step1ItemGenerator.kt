package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class Step1ItemGenerator {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelay(source: StartItem, maxDelayInNanos: Int): Step1Item {
        Thread.sleep(0, random.nextInt(maxDelayInNanos))
        return Step1Item(source, id.getAndIncrement())
    }

}
package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class Step2ItemGenerator {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelay(source: Step1Item, maxDelayInNanos: Int): Step2Item {
        Thread.sleep(0, random.nextInt(maxDelayInNanos))
        return Step2Item(source, id.getAndIncrement())
    }

}
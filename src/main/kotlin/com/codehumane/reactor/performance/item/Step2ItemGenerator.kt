package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class Step2ItemGenerator {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelayMillis(source: Step1Item, maxDelayInMillis: Int): Step2Item {
        Thread.sleep(random.nextInt(maxDelayInMillis).toLong())
        return Step2Item(source, id.getAndIncrement())
    }

}
package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class FinalItemGenerator(val name: String) {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelayMillis(source: Step2Item, maxDelayInMillis: Int): FinalItem {
        Thread.sleep(random.nextInt(maxDelayInMillis).toLong())
        return FinalItem(source, "$name-${id.getAndIncrement()}")
    }

}
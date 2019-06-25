package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class Step1ItemGenerator {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelay(source: StartItem): Step1Item {
        Thread.sleep(0, random.nextInt(100_000)) // 최대 0.1ms
        return Step1Item(source, id.getAndIncrement())
    }

}
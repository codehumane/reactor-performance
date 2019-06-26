package com.codehumane.reactor.performance.item

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class StartItemGenerator {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelay(maxDelayInNanos: Int): StartItem {
//        Thread.sleep(0, random.nextInt(maxDelayInNanos))
        return StartItem(id.getAndIncrement())
    }

}

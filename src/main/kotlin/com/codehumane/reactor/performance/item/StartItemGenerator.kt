package com.codehumane.reactor.performance.item

import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger

class StartItemGenerator {

    private val log = LoggerFactory.getLogger(StartItemGenerator::class.java)
    private val id = AtomicInteger(0)

    fun withDelayCount(delayLoopCount: Int): StartItem {
        (0 until delayLoopCount).forEach {
            log.debug("generate delay count : $it")
        }

        return StartItem(id.getAndIncrement())
    }

}

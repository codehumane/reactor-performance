package com.codehumane.reactor.performance.metric

import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

class TPSCollector(private val maxCollectCount: Int, private val source: Map<String, Timer>) {

    private val log = LoggerFactory.getLogger(TPSCollector::class.java)
    private val tpsCollection = source.mapValues { TPS(it.key, maxCollectCount) }
    private var running = false

    fun start() {

        if (running) return
        running = true

        CompletableFuture.runAsync {
            var accumulatedCount = 1

            while (true) {
                Thread.sleep(1000)
                collect()

                if (accumulatedCount++ == maxCollectCount) {
                    log.info(describe())
                    accumulatedCount = 0
                }
            }
        }
        
    }

    private fun collect() {
        tpsCollection.forEach {
            it.value.add(source.getValue(it.key).count())
        }
    }

    private fun describe(): String {
        return tpsCollection.map { it.value.describe() }.joinToString("\n")
    }


}

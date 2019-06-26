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

            var accumulatedCount = 0

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


    data class TPS(private val name: String, private val maxCollectCount: Int) {

        val collection = mutableListOf<Long>()

        fun add(count: Long) {
            collection.add(count)
            if (collection.size > maxCollectCount) collection.removeAt(0)
        }

        fun describe(): String {
            if (collection.size < 2) return "not enough data"

            val tps = mutableListOf<Long>()
            (1 until collection.size).forEach { tps.add(collection[it] - collection[it - 1]) }
            return "$name tps: ${tps.average()}, detail: ${tps.joinToString()}"
        }
    }

}

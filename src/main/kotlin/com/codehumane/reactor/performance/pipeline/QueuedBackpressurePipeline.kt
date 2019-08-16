package com.codehumane.reactor.performance.pipeline

import org.springframework.util.StopWatch
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.scheduler.Schedulers
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit

class QueuedBackpressurePipeline(maxQueueSize: Int, private val sleepInMilliseconds: Long) {

    private val queue = LinkedBlockingDeque<Int>(maxQueueSize)
    private val flux = Flux
        .create(readQueueAsync())
        .map(logItemAfterSleep())
        .subscribeOn(Schedulers.parallel())

    private fun readQueueAsync(): (t: FluxSink<Int>) -> Unit {
        return {
            while (true) {
                val watch = StopWatch("queue#take")
                watch.start()
                val item = queue.take()
                watch.stop()
                println("queue#take item(${watch.lastTaskTimeMillis}ms): $item")
                it.next(item)
            }
        }
    }

    private fun logItemAfterSleep(): (t: Int) -> Unit {
        return {
            TimeUnit.MILLISECONDS.sleep(sleepInMilliseconds)
            println("map item: $it")
        }
    }

    fun start() {
        flux.subscribe()
    }

    fun add(item: Int) {
        val watch = StopWatch("queue#put")
        watch.start()
        queue.put(item)
        watch.stop()
        println("queue#put item(${watch.lastTaskTimeMillis}ms): $item")
    }

}
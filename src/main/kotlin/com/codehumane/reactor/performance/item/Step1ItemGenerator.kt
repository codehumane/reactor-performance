package com.codehumane.reactor.performance.item

import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class Step1ItemGenerator(private val scheduler: Scheduler) {

    private val id = AtomicInteger(0)
    private val random = Random()

    fun withDelay(start: StartItem): Mono<Step1Item> {
        val item = Step1Item(start, id.getAndIncrement())
        val delay = random.nextInt(100_000).toLong()

        return Mono
            .just(item)
            .delayElement(Duration.ofNanos(delay))
            .subscribeOn(scheduler)
    }

}
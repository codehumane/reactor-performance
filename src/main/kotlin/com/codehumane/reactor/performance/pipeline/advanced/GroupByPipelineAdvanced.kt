package com.codehumane.reactor.performance.pipeline.advanced

import com.codehumane.reactor.performance.item.*
import com.codehumane.reactor.performance.metric.TPSCollector
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.CompletableFuture
import kotlin.math.abs
import kotlin.system.exitProcess

/**
 * binary log event를 받아서 replicate item으로 변환하고 각 샤드 DB로 복제하는 일련의 파이프라인을 구성
 * `com.codehumane.reactor.performance.pipeline.GroupByPipeline`을 이용하여 좀 더 복잡한 작업을 수행
 *
 * <결론>
 *
<시작하고 두 20초 뒤>

start tps: 10500.0, detail: 10590, 10670, 10478, 10538, 10531, 10693, 10594, 10493, 9913
step1 tps: 10500.0, detail: 10590, 10670, 10478, 10538, 10531, 10693, 10594, 10492, 9914
step2 tps: 10500.555555555555, detail: 10590, 10665, 10488, 10530, 10534, 10692, 10598, 10491, 9917
pipeline_final_0 tps: 656.1111111111111, detail: 659, 669, 655, 658, 658, 669, 661, 656, 620
pipeline_final_1 tps: 656.4444444444445, detail: 665, 665, 656, 657, 660, 669, 659, 658, 619
pipeline_final_2 tps: 656.2222222222222, detail: 663, 664, 656, 660, 657, 670, 659, 657, 620
pipeline_final_3 tps: 656.1111111111111, detail: 662, 666, 657, 658, 656, 672, 658, 659, 617
pipeline_final_4 tps: 656.1111111111111, detail: 662, 666, 657, 657, 659, 665, 664, 655, 620
pipeline_final_5 tps: 656.2222222222222, detail: 658, 668, 658, 657, 660, 668, 660, 658, 619
pipeline_final_6 tps: 656.4444444444445, detail: 660, 667, 657, 656, 661, 666, 665, 655, 621
pipeline_final_7 tps: 656.2222222222222, detail: 660, 666, 655, 657, 659, 669, 662, 656, 622
pipeline_final_8 tps: 656.2222222222222, detail: 663, 666, 657, 655, 661, 667, 661, 658, 618
pipeline_final_9 tps: 656.7777777777778, detail: 664, 667, 654, 659, 657, 667, 663, 658, 622
pipeline_final_10 tps: 656.7777777777778, detail: 664, 667, 655, 660, 656, 668, 661, 660, 620
pipeline_final_11 tps: 656.5555555555555, detail: 663, 667, 657, 658, 659, 664, 663, 656, 622
pipeline_final_12 tps: 656.5555555555555, detail: 662, 668, 656, 659, 657, 668, 662, 654, 623
pipeline_final_13 tps: 656.4444444444445, detail: 663, 663, 659, 657, 658, 668, 662, 657, 621
pipeline_final_14 tps: 656.1111111111111, detail: 660, 666, 655, 658, 658, 670, 663, 654, 621
pipeline_final_15 tps: 656.4444444444445, detail: 663, 664, 658, 658, 657, 668, 662, 656, 622

<거의 끝날 무렵>
마찬가지
 */
@Service
class GroupByPipelineAdvanced(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(GroupByPipelineAdvanced::class.java)

    private val intermediateTransformThreadCoreSize = 128
    private val topicSubscriberCount = 16
    private val topicSubscriberChildrenCount = 8

    private val itemGenerator = StartItemGenerator()
    private val step1Generator = Step1ItemGenerator()
    private val step2Generator = Step2ItemGenerator()
    private val finalGenerators = (0 until topicSubscriberCount)
        .map { FinalItemGenerator(it.toString()) }

    private val intermediateTransformScheduler = scheduler(intermediateTransformThreadCoreSize, 32, "step2-")
    private val finalExecutors = (0 until topicSubscriberCount).associateBy({ it }, { finalIdx ->
        (0 until topicSubscriberChildrenCount).map { childIdx ->
            executor(1, 10_000, "final-$finalIdx-child-$childIdx")
        }
    })

    private val startMetricTimer = meterRegistry.timer("pipeline_start")
    private val step1MetricTimer = meterRegistry.timer("pipeline_step1")
    private val step2MetricTimer = meterRegistry.timer("pipeline_step2")
    private val finalMetricTimers = (0 until topicSubscriberCount)
        .map { meterRegistry.timer("pipeline_final_$it") }


    /**
     * 파이프라인 실행 (구독)
     */
    fun start(publishItemCount: Int) {

        startTpsCollector()

        // start item publish & intermediate transform
        Flux
            .create<StartItem>({ startItemPublishAsynchronously(it, publishItemCount) }, BUFFER)
            .flatMapSequential<Step2Item>(this::transform, intermediateTransformThreadCoreSize, 1)
            .groupBy { it.value.rem(topicSubscriberCount) }
            .doOnError(this::terminateOnUnrecoverableError)
            .subscribe { grouped ->

                grouped
                    .doOnNext { generateFinalItem(it, grouped.key()!!) }
                    .doOnError(this::terminateOnUnrecoverableError)
                    .subscribe()

            }
    }

    private fun scheduler(corePoolSize: Int, queueCapacity: Int, namePrefix: String): Scheduler {
        val executor = executor(corePoolSize, queueCapacity, namePrefix)
        return Schedulers.fromExecutorService(executor.threadPoolExecutor)
    }

    private fun executor(corePoolSize: Int, queueCapacity: Int, namePrefix: String): ThreadPoolTaskExecutor {
        return ThreadPoolTaskExecutor().apply {
            this.corePoolSize = corePoolSize
            this.maxPoolSize = corePoolSize
            setQueueCapacity(queueCapacity)
            setThreadNamePrefix(namePrefix)
            initialize()
        }
    }

    private fun startItemPublishAsynchronously(sink: FluxSink<StartItem>, count: Int) {
        log.info("play ground publishing source")
        CompletableFuture.runAsync {

            (0 until count).forEach { index ->
                startMetricTimer.record {
                    sink.next(itemGenerator.withDelayCount(1_000))
                }

                if (index % 1000 == 0) {
                    log.info("$index items published.")
                }
            }

            log.info("item publishing finished.")
        }
    }

    private fun transform(source: StartItem): Mono<Step2Item> {
        return Mono.create<Step2Item> {

            val step1Item = step1MetricTimer.record<Step1Item> {
                step1Generator.withDelayMillis(source, 1)
            }

            step2MetricTimer.record {
                val step2Item = step2Generator.withDelayMillis(step1Item, 5)
                it.success(step2Item)
            }

        }.subscribeOn(intermediateTransformScheduler)

    }

    private fun generateFinalItem(source: Step2Item, parentIndex: Int) {
        val childrenExecutors = finalExecutors[parentIndex] ?: error("")
        val childIndex = abs("child-${source.value}".hashCode()).rem(childrenExecutors.size)
        val executor = childrenExecutors[childIndex]
        val runnable = FinalItemGenerateRunnable(
            finalGenerators[parentIndex],
            finalMetricTimers[parentIndex],
            source,
            parentIndex,
            childIndex
        )

        executor.execute(runnable)
    }

    private fun terminateOnUnrecoverableError(it: Throwable?) {
        log.error("unrecoverable error. system exit", it)
        exitProcess(666)
    }

    private fun startTpsCollector() {
        val tpsCollectorSource = mutableMapOf(
            "start" to startMetricTimer,
            "step1" to step1MetricTimer,
            "step2" to step2MetricTimer
        )

        finalMetricTimers.forEach {
            tpsCollectorSource[it.id.name] = it
        }

        TPSCollector(10, tpsCollectorSource).start()
    }

    private inner class FinalItemGenerateRunnable(
        private val generator: FinalItemGenerator,
        private val timer: Timer,
        private val source: Step2Item,
        private val parentIndex: Int,
        private val childIndex: Int
    ) : Runnable {

        override fun run() {
            return timer.record {
                val generated = generator.withDelayMillis(source, 10)
//                log.info("generated(${source.value}-$parentIndex-$childIndex): $generated")
            }
        }

    }
}
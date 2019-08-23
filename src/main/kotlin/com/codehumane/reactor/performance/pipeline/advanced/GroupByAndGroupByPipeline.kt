package com.codehumane.reactor.performance.pipeline.advanced

import com.codehumane.reactor.performance.item.*
import com.codehumane.reactor.performance.metric.TPSCollector
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER
import reactor.core.publisher.GroupedFlux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.CompletableFuture
import kotlin.math.abs
import kotlin.system.exitProcess

/**
 * binary log event를 받아서 replicate item으로 변환하고 각 샤드 DB로 복제하는 일련의 파이프라인을 구성
 * `com.codehumane.reactor.performance.pipeline.advanced.GroupByAndGroupByPipeline`을 이용하여 좀 더 복잡한 작업을 수행
 *
 * <결론>
 *
<시작하고 두 20초 뒤>

start tps: 10381.222222222223, detail: 10532, 9930, 10331, 10425, 10449, 10462, 10804, 10262, 10236
step1 tps: 10381.222222222223, detail: 10532, 9930, 10331, 10425, 10449, 10462, 10804, 10262, 10236
step2 tps: 10381.222222222223, detail: 10532, 9930, 10331, 10424, 10450, 10462, 10804, 10262, 10236
pipeline_final_0 tps: 648.6666666666666, detail: 658, 620, 646, 651, 653, 654, 676, 641, 639
pipeline_final_1 tps: 648.8888888888889, detail: 659, 620, 645, 652, 653, 654, 676, 640, 641
pipeline_final_2 tps: 648.8888888888889, detail: 658, 621, 646, 651, 653, 655, 675, 642, 639
pipeline_final_3 tps: 648.7777777777778, detail: 658, 621, 645, 652, 652, 655, 674, 641, 641
pipeline_final_4 tps: 649.0, detail: 659, 621, 645, 652, 653, 654, 675, 641, 641
pipeline_final_5 tps: 648.7777777777778, detail: 658, 621, 646, 650, 654, 654, 676, 641, 639
pipeline_final_6 tps: 648.8888888888889, detail: 659, 621, 645, 652, 653, 655, 673, 643, 639
pipeline_final_7 tps: 648.7777777777778, detail: 658, 621, 645, 652, 653, 655, 674, 642, 639
pipeline_final_8 tps: 649.0, detail: 658, 622, 645, 653, 652, 653, 676, 641, 641
pipeline_final_9 tps: 649.0, detail: 658, 622, 645, 651, 654, 654, 676, 641, 640
pipeline_final_10 tps: 648.7777777777778, detail: 658, 621, 645, 652, 652, 655, 675, 642, 639
pipeline_final_11 tps: 648.8888888888889, detail: 660, 619, 646, 652, 653, 653, 676, 641, 640
pipeline_final_12 tps: 648.6666666666666, detail: 657, 622, 644, 653, 653, 653, 676, 641, 639
pipeline_final_13 tps: 648.8888888888889, detail: 659, 621, 646, 650, 653, 655, 674, 642, 640
pipeline_final_14 tps: 648.7777777777778, detail: 658, 621, 645, 653, 653, 653, 676, 641, 639
pipeline_final_15 tps: 649.0, detail: 659, 620, 647, 651, 653, 654, 675, 642, 640

<거의 끝날 무렵>
마찬가지
 */
@Service
class GroupByAndGroupByPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(GroupByAndGroupByPipeline::class.java)

    private val intermediateTransformThreadCoreSize = 128
    private val topicSubscriberCount = 16
    private val topicSubscriberChildrenCount = 8

    private val itemGenerator = StartItemGenerator()
    private val step1Generator = Step1ItemGenerator()
    private val step2Generator = Step2ItemGenerator()
    private val finalGenerators = (0 until topicSubscriberCount)
        .map { FinalItemGenerator(it.toString()) }

    private val intermediateTransformScheduler = scheduler(intermediateTransformThreadCoreSize, 128, "step2-")
    private val finalSchedulers = (0 until topicSubscriberCount).associateBy({ it }, { parentIdx ->
        (0 until topicSubscriberChildrenCount).map { childIdx ->
            scheduler(1, 32, "final-$parentIdx-child-$childIdx")
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

                /**
                 * 샤드 별로 나눔
                 */
                grouped
                    .groupBy { abs("child-${it.value}".hashCode()).rem(topicSubscriberChildrenCount) }
                    .doOnError(this::terminateOnUnrecoverableError)
                    .subscribe { groupedAndGrouped ->

                        /**
                         * 샤드 안에서도 같은 레코드 별로 나눔
                         * (업데이트 꼬이지 않게 하기 위함)
                         */
                        groupedAndGrouped
                            .publishOn(finalSchedulers.getValue(grouped.key()!!)[groupedAndGrouped.key()!!])
                            .doOnError(this::terminateOnUnrecoverableError)
                            .subscribe(generateFinalItem(grouped, groupedAndGrouped))

                    }
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
                val step2Item = step2Generator.withDelayMillis(step1Item, 1)
                it.success(step2Item)
            }

        }.subscribeOn(intermediateTransformScheduler)

    }

    private fun generateFinalItem(
        grouped: GroupedFlux<Int, Step2Item>,
        groupedAndGrouped: GroupedFlux<Int, Step2Item>
    ): (t: Step2Item) -> Unit {
        return {

            val generator = finalGenerators[grouped.key()!!]
            val timer = finalMetricTimers[grouped.key()!!]

            timer.record {
                val generated = generator.withDelayMillis(it, 3)
//                log.info("generated(${it.value}-${grouped.key()}-${groupedAndGrouped.key()}): $generated")
            }

        }
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

}
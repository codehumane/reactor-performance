package com.codehumane.reactor.performance.pipeline

import com.codehumane.reactor.performance.item.*
import com.codehumane.reactor.performance.metric.TPSCollector
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
import kotlin.system.exitProcess

/**
 * binary log event를 받아서 replicate item으로 변환하고 각 샤드 DB로 복제하는 일련의 파이프라인을 구성
 *
 * `OrderingStepMinifiedNonTopicProcessorPipeline`를 변형했으며,
 * 이미 제공되는 groupBy를 사용하여 좀 더 단순하게 구현해 봄. (fanout 방식 보다 단순함)
 *
 * <결론>
 *
<시작하고 두 20초 뒤>

start tps: 10311.777777777777, detail: 10487, 10491, 10280, 10023, 10236, 10233, 10024, 10478, 10554
step1 tps: 10311.777777777777, detail: 10487, 10491, 10280, 10023, 10236, 10233, 10024, 10478, 10554
step2 tps: 10312.111111111111, detail: 10486, 10500, 10275, 10030, 10232, 10235, 10018, 10481, 10552
pipeline_final_0 tps: 634.6666666666666, detail: 647, 637, 638, 627, 630, 630, 625, 643, 635
pipeline_final_1 tps: 634.8888888888889, detail: 643, 633, 642, 616, 629, 641, 617, 643, 650
pipeline_final_2 tps: 635.3333333333334, detail: 647, 646, 644, 615, 630, 636, 620, 636, 644
pipeline_final_3 tps: 634.2222222222222, detail: 627, 651, 626, 624, 633, 625, 621, 646, 655
pipeline_final_4 tps: 636.6666666666666, detail: 650, 649, 634, 619, 632, 629, 618, 649, 650
pipeline_final_5 tps: 631.6666666666666, detail: 641, 639, 627, 623, 621, 625, 618, 650, 641
pipeline_final_6 tps: 635.4444444444445, detail: 647, 640, 636, 626, 632, 630, 616, 642, 650
pipeline_final_7 tps: 634.5555555555555, detail: 652, 642, 636, 618, 624, 634, 617, 631, 657
pipeline_final_8 tps: 636.2222222222222, detail: 650, 643, 640, 625, 628, 629, 616, 645, 650
pipeline_final_9 tps: 635.3333333333334, detail: 645, 636, 640, 624, 621, 639, 627, 635, 651
pipeline_final_10 tps: 637.5555555555555, detail: 652, 645, 632, 618, 633, 639, 617, 650, 652
pipeline_final_11 tps: 636.2222222222222, detail: 648, 648, 632, 620, 626, 632, 625, 638, 657
pipeline_final_12 tps: 636.0, detail: 645, 644, 630, 620, 635, 639, 620, 644, 647
pipeline_final_13 tps: 634.4444444444445, detail: 646, 649, 628, 615, 626, 623, 619, 653, 651
pipeline_final_14 tps: 635.0, detail: 650, 642, 638, 625, 616, 629, 617, 644, 654
pipeline_final_15 tps: 631.4444444444445, detail: 646, 630, 626, 626, 631, 636, 614, 638, 636

<거의 끝날 무렵>
마찬가지
 */
@Service
class GroupByPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(GroupByPipeline::class.java)

    private val intermediateTransformThreadCoreSize = 128
    private val finalItemThreadCoreSize = 4
    private val topicSubscriberCount = 16

    private val itemGenerator = StartItemGenerator()
    private val step1Generator = Step1ItemGenerator()
    private val step2Generator = Step2ItemGenerator()
    private val finalGenerators = (0 until topicSubscriberCount)
        .map { FinalItemGenerator(it.toString()) }

    private val intermediateTransformScheduler = scheduler(intermediateTransformThreadCoreSize, 32, "step2-")
    private val finalSchedulers = (0 until topicSubscriberCount)
        .map { scheduler(finalItemThreadCoreSize, 2, "final-$it-") }

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
//                    .take(Duration.ofMillis(100))
                    .flatMap({ generateFinalItem(it, grouped.key()!!) }, finalItemThreadCoreSize, 1)
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

    private fun generateFinalItem(source: Step2Item, index: Int): Mono<FinalItem> {
        val generator = finalGenerators[index]
        val scheduler = finalSchedulers[index]
        val timer = finalMetricTimers[index]

        return Mono.create<FinalItem> {
            timer.record {
                it.success(generator.withDelayMillis(source, 10))
            }
        }.subscribeOn(scheduler)
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
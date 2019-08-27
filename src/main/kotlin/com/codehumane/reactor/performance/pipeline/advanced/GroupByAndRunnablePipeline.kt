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

start tps: 19401.333333333332, detail: 19707, 19496, 19656, 19691, 19694, 19283, 19049, 18690, 19346
step1 tps: 19401.333333333332, detail: 19707, 19495, 19657, 19691, 19694, 19283, 19049, 18690, 19346
step2 tps: 19401.11111111111, detail: 19715, 19492, 19651, 19692, 19695, 19279, 19057, 18686, 19343
pipeline_final_0 tps: 1213.2222222222222, detail: 1231, 1220, 1235, 1224, 1237, 1206, 1183, 1176, 1207
pipeline_final_1 tps: 1213.0, detail: 1235, 1216, 1229, 1231, 1233, 1201, 1188, 1173, 1211
pipeline_final_2 tps: 1213.111111111111, detail: 1229, 1215, 1236, 1230, 1229, 1201, 1191, 1181, 1206
pipeline_final_3 tps: 1213.4444444444443, detail: 1237, 1212, 1229, 1231, 1231, 1209, 1184, 1174, 1214
pipeline_final_4 tps: 1213.111111111111, detail: 1239, 1207, 1233, 1236, 1224, 1215, 1189, 1166, 1209
pipeline_final_5 tps: 1212.4444444444443, detail: 1232, 1220, 1223, 1233, 1234, 1201, 1196, 1163, 1210
pipeline_final_6 tps: 1212.2222222222222, detail: 1229, 1216, 1237, 1220, 1235, 1211, 1186, 1169, 1207
pipeline_final_7 tps: 1212.6666666666667, detail: 1234, 1213, 1229, 1236, 1228, 1195, 1196, 1175, 1208
pipeline_final_8 tps: 1212.111111111111, detail: 1231, 1211, 1233, 1225, 1232, 1212, 1185, 1174, 1206
pipeline_final_9 tps: 1212.0, detail: 1228, 1214, 1229, 1239, 1219, 1217, 1187, 1163, 1212
pipeline_final_10 tps: 1212.2222222222222, detail: 1232, 1209, 1231, 1237, 1230, 1206, 1191, 1172, 1202
pipeline_final_11 tps: 1212.111111111111, detail: 1233, 1208, 1228, 1236, 1229, 1205, 1193, 1172, 1205
pipeline_final_12 tps: 1213.888888888889, detail: 1245, 1218, 1228, 1231, 1231, 1207, 1183, 1177, 1205
pipeline_final_13 tps: 1212.2222222222222, detail: 1230, 1216, 1227, 1238, 1226, 1209, 1193, 1174, 1197
pipeline_final_14 tps: 1212.6666666666667, detail: 1233, 1211, 1230, 1232, 1234, 1207, 1187, 1176, 1204
pipeline_final_15 tps: 1212.7777777777778, detail: 1234, 1206, 1237, 1233, 1232, 1204, 1186, 1171, 1212

<거의 끝날 무렵>
마찬가지
 */
@Service
class GroupByAndRunnablePipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(GroupByAndRunnablePipeline::class.java)

    private val intermediateTransformThreadCoreSize = 128
    private val topicSubscriberCount = 16
    private val topicSubscriberChildrenCount = 8
    private val startItemDelay = 500
    private val step1ItemDelay = 1
    private val step2ItemDelay = 1
    private val finalItemDelay = 2

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
                    sink.next(itemGenerator.withDelayCount(startItemDelay))
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
                step1Generator.withDelayMillis(source, step1ItemDelay)
            }

            step2MetricTimer.record {
                val step2Item = step2Generator.withDelayMillis(step1Item, step2ItemDelay)
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
                val generated = generator.withDelayMillis(source, finalItemDelay)
//                log.info("generated(${source.value}-$parentIndex-$childIndex): $generated")
            }
        }

    }
}
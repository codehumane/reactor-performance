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

start tps: 19374.0, detail: 18991, 19702, 19394, 19553, 19414, 19206, 19483, 19293, 19330
step1 tps: 19374.0, detail: 18991, 19702, 19394, 19553, 19414, 19206, 19483, 19293, 19330
step2 tps: 19374.555555555555, detail: 18984, 19712, 19389, 19557, 19413, 19197, 19491, 19288, 19340
pipeline_final_0 tps: 1211.5555555555557, detail: 1185, 1231, 1218, 1222, 1213, 1196, 1218, 1206, 1215
pipeline_final_1 tps: 1211.0, detail: 1181, 1227, 1224, 1220, 1210, 1199, 1219, 1203, 1216
pipeline_final_2 tps: 1211.3333333333333, detail: 1189, 1228, 1219, 1222, 1211, 1198, 1224, 1206, 1205
pipeline_final_3 tps: 1211.3333333333333, detail: 1185, 1232, 1209, 1224, 1217, 1193, 1221, 1210, 1211
pipeline_final_4 tps: 1210.4444444444443, detail: 1192, 1223, 1210, 1226, 1217, 1193, 1223, 1209, 1201
pipeline_final_5 tps: 1211.0, detail: 1187, 1233, 1214, 1227, 1206, 1195, 1228, 1193, 1216
pipeline_final_6 tps: 1211.0, detail: 1188, 1231, 1204, 1233, 1208, 1202, 1218, 1204, 1211
pipeline_final_7 tps: 1211.3333333333333, detail: 1188, 1231, 1213, 1223, 1210, 1203, 1214, 1209, 1211
pipeline_final_8 tps: 1211.7777777777778, detail: 1193, 1231, 1202, 1232, 1211, 1199, 1217, 1212, 1209
pipeline_final_9 tps: 1210.888888888889, detail: 1195, 1220, 1215, 1228, 1211, 1198, 1219, 1206, 1206
pipeline_final_10 tps: 1211.5555555555557, detail: 1186, 1236, 1206, 1227, 1204, 1191, 1236, 1206, 1212
pipeline_final_11 tps: 1211.7777777777778, detail: 1185, 1244, 1203, 1226, 1212, 1200, 1220, 1204, 1212
pipeline_final_12 tps: 1210.6666666666667, detail: 1182, 1234, 1210, 1227, 1212, 1192, 1229, 1199, 1211
pipeline_final_13 tps: 1211.6666666666667, detail: 1197, 1225, 1215, 1228, 1208, 1200, 1215, 1211, 1206
pipeline_final_14 tps: 1211.3333333333333, detail: 1192, 1225, 1214, 1225, 1212, 1196, 1218, 1204, 1216
pipeline_final_15 tps: 1210.6666666666667, detail: 1184, 1231, 1212, 1227, 1210, 1199, 1214, 1207, 1212

<거의 끝날 무렵>
마찬가지
 */
@Service
class GroupByAndGroupByPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(GroupByAndGroupByPipeline::class.java)

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

    private fun generateFinalItem(
        grouped: GroupedFlux<Int, Step2Item>,
        groupedAndGrouped: GroupedFlux<Int, Step2Item>
    ): (t: Step2Item) -> Unit {
        return {

            val generator = finalGenerators[grouped.key()!!]
            val timer = finalMetricTimers[grouped.key()!!]

            timer.record {
                val generated = generator.withDelayMillis(it, finalItemDelay)
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
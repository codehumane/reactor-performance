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
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.CompletableFuture
import kotlin.math.abs
import kotlin.system.exitProcess

/**
 * binary log event를 받아서 replicate item으로 변환하고 각 샤드 DB로 복제하는 일련의 파이프라인을 구성
 * publish를 두 번 사용하는 파이프라인 (GroupByAndGroupByPipeline과 하는 일 동일함)
 *
 * <결론>
 *
<시작하고 두 20초 뒤>

start tps: 10820.444444444445, detail: 10501, 10876, 11037, 10875, 10789, 10865, 10920, 10806, 10715
step1 tps: 10820.555555555555, detail: 10502, 10876, 11037, 10875, 10788, 10866, 10920, 10806, 10715
step2 tps: 10820.555555555555, detail: 10502, 10876, 11037, 10875, 10788, 10866, 10920, 10806, 10715
pipeline_final_0 tps: 676.2222222222222, detail: 657, 680, 689, 680, 674, 680, 681, 677, 668
pipeline_final_1 tps: 676.3333333333334, detail: 657, 679, 690, 680, 674, 680, 681, 676, 670
pipeline_final_2 tps: 676.3333333333334, detail: 657, 680, 688, 681, 674, 679, 683, 675, 670
pipeline_final_3 tps: 676.2222222222222, detail: 655, 680, 691, 679, 675, 679, 682, 675, 670
pipeline_final_4 tps: 676.3333333333334, detail: 656, 681, 689, 680, 675, 678, 683, 676, 669
pipeline_final_5 tps: 676.1111111111111, detail: 655, 680, 691, 679, 673, 680, 683, 674, 670
pipeline_final_6 tps: 676.2222222222222, detail: 655, 681, 690, 679, 675, 679, 682, 675, 670
pipeline_final_7 tps: 676.2222222222222, detail: 656, 680, 690, 679, 675, 678, 683, 676, 669
pipeline_final_8 tps: 676.2222222222222, detail: 657, 678, 691, 679, 675, 678, 684, 674, 670
pipeline_final_9 tps: 676.2222222222222, detail: 657, 678, 691, 679, 675, 678, 682, 677, 669
pipeline_final_10 tps: 676.1111111111111, detail: 657, 679, 690, 679, 674, 680, 682, 675, 669
pipeline_final_11 tps: 676.2222222222222, detail: 656, 680, 690, 678, 676, 678, 683, 676, 669
pipeline_final_12 tps: 676.1111111111111, detail: 656, 680, 689, 680, 675, 678, 684, 674, 669
pipeline_final_13 tps: 676.2222222222222, detail: 657, 680, 689, 680, 674, 679, 684, 674, 669
pipeline_final_14 tps: 676.3333333333334, detail: 656, 680, 690, 680, 675, 678, 682, 675, 671
pipeline_final_15 tps: 676.3333333333334, detail: 656, 680, 690, 680, 673, 680, 682, 677, 669

<거의 끝날 무렵>
마찬가지
 */
@Service
class PublishAndPublishPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(PublishAndPublishPipeline::class.java)

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
        val receiveAndTransformPipeline = Flux
            .create<StartItem>({ startItemPublishAsynchronously(it, publishItemCount) }, BUFFER)
            .flatMapSequential<Step2Item>(this::transform, intermediateTransformThreadCoreSize, 1)
            .doOnError(this::terminateOnUnrecoverableError)
            .publish()

        (0 until topicSubscriberCount).forEach { parentIndex ->
            val shardNumberFilterPipeline = receiveAndTransformPipeline
                .filter { it.value.rem(topicSubscriberCount) == parentIndex }
                .doOnError(this::terminateOnUnrecoverableError)
                .publish()

            (0 until topicSubscriberChildrenCount).forEach { childIndex ->
                shardNumberFilterPipeline
                    .filter { abs("child-${it.value}".hashCode()).rem(topicSubscriberChildrenCount) == childIndex }
                    .publishOn(finalSchedulers.getValue(parentIndex)[childIndex])
                    .doOnError(this::terminateOnUnrecoverableError)
                    .subscribe(generateFinalItem(parentIndex, childIndex))
            }

            shardNumberFilterPipeline.connect()
        }

        receiveAndTransformPipeline.connect()
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

    private fun generateFinalItem(parentIndex: Int, childIndex: Int): (t: Step2Item) -> Unit {
        return {
            val generator = finalGenerators[parentIndex]
            val timer = finalMetricTimers[parentIndex]

            timer.record {
                val generated = generator.withDelayMillis(it, 3)
//                log.info("generated(${it.value}-$parentIndex-$childIndex): $generated")
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
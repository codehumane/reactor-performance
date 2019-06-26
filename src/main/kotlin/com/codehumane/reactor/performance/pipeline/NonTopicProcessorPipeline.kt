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
<시작하고 두 20초 뒤>
start tps: 10432.888888888889, detail: 9999, 9811, 10299, 10859, 10791, 10591, 10437, 10630, 10479
step1 tps: 4062.6666666666665, detail: 4163, 3894, 4048, 4158, 4099, 4077, 4012, 4049, 4064
step2 tps: 4062.8888888888887, detail: 4162, 3896, 4043, 4165, 4099, 4076, 4012, 4048, 4065
pipeline_final_0 tps: 507.77777777777777, detail: 520, 487, 504, 520, 515, 508, 501, 504, 511
pipeline_final_1 tps: 508.3333333333333, detail: 524, 486, 506, 520, 512, 509, 504, 503, 511
pipeline_final_2 tps: 507.77777777777777, detail: 521, 488, 505, 518, 514, 508, 503, 507, 506
pipeline_final_3 tps: 507.77777777777777, detail: 519, 486, 506, 520, 513, 509, 503, 505, 509
pipeline_final_4 tps: 507.6666666666667, detail: 520, 485, 509, 516, 514, 509, 505, 503, 508
pipeline_final_5 tps: 507.77777777777777, detail: 520, 486, 505, 520, 514, 508, 504, 506, 507
pipeline_final_6 tps: 508.22222222222223, detail: 523, 485, 506, 518, 515, 508, 503, 506, 510
pipeline_final_7 tps: 508.1111111111111, detail: 522, 486, 508, 519, 511, 511, 500, 507, 509
pipeline_final_8 tps: 507.77777777777777, detail: 523, 485, 506, 521, 514, 509, 499, 507, 506
pipeline_final_9 tps: 508.0, detail: 521, 487, 506, 519, 513, 509, 504, 505, 508
pipeline_final_10 tps: 508.0, detail: 522, 485, 506, 520, 513, 509, 503, 503, 511
pipeline_final_11 tps: 508.1111111111111, detail: 522, 486, 504, 523, 511, 509, 502, 504, 512
pipeline_final_12 tps: 507.8888888888889, detail: 522, 486, 504, 521, 512, 510, 503, 504, 509
pipeline_final_13 tps: 507.6666666666667, detail: 520, 487, 504, 522, 511, 508, 506, 503, 508
pipeline_final_14 tps: 507.77777777777777, detail: 520, 488, 505, 521, 511, 510, 502, 505, 508
pipeline_final_15 tps: 508.1111111111111, detail: 523, 486, 504, 522, 512, 509, 502, 504, 511

<거의 끝날 무렵>
start tps: 10062.444444444445, detail: 10222, 10202, 10232, 10156, 10056, 9027, 10199, 10199, 10269
step1 tps: 3831.222222222222, detail: 3898, 3905, 3901, 3866, 3863, 3275, 3918, 3927, 3928
step2 tps: 3830.8888888888887, detail: 3902, 3903, 3896, 3873, 3857, 3272, 3921, 3929, 3925
pipeline_final_0 tps: 478.6666666666667, detail: 487, 487, 487, 486, 482, 409, 490, 490, 490
pipeline_final_1 tps: 479.0, detail: 487, 488, 487, 485, 483, 407, 491, 490, 493
pipeline_final_2 tps: 478.77777777777777, detail: 484, 491, 487, 484, 483, 409, 490, 491, 490
pipeline_final_3 tps: 478.77777777777777, detail: 486, 489, 487, 484, 480, 412, 489, 491, 491
pipeline_final_4 tps: 478.8888888888889, detail: 488, 488, 487, 483, 486, 407, 490, 492, 489
pipeline_final_5 tps: 479.0, detail: 490, 489, 485, 484, 483, 409, 490, 490, 491
pipeline_final_6 tps: 479.0, detail: 489, 488, 487, 484, 482, 409, 489, 493, 490
pipeline_final_7 tps: 478.77777777777777, detail: 487, 491, 488, 483, 482, 408, 491, 491, 488
pipeline_final_8 tps: 479.0, detail: 488, 488, 488, 483, 483, 409, 489, 494, 489
pipeline_final_9 tps: 478.55555555555554, detail: 486, 487, 489, 482, 486, 405, 493, 489, 490
pipeline_final_10 tps: 479.22222222222223, detail: 489, 487, 486, 487, 481, 409, 490, 491, 493
pipeline_final_11 tps: 478.8888888888889, detail: 485, 487, 488, 485, 483, 408, 491, 491, 492
pipeline_final_12 tps: 479.22222222222223, detail: 488, 487, 490, 483, 483, 409, 490, 489, 494
pipeline_final_13 tps: 478.8888888888889, detail: 488, 488, 489, 481, 484, 410, 490, 491, 489
pipeline_final_14 tps: 478.55555555555554, detail: 487, 488, 487, 482, 483, 410, 490, 490, 490
pipeline_final_15 tps: 478.6666666666667, detail: 488, 486, 488, 485, 483, 405, 494, 491, 488
 */
@Service
class NonTopicProcessorPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(NonTopicProcessorPipeline::class.java)

    private val step1ThreadCoreSize = 32
    private val step2ThreadCoreSize = 32
    private val finalItemThreadCoreSize = 4
    private val topicSubscriberCount = 16
    private val processor = Processor()

    private val itemGenerator = StartItemGenerator()
    private val step1Generator = Step1ItemGenerator()
    private val step2Generator = Step2ItemGenerator()
    private val finalGenerators = (0 until topicSubscriberCount)
        .map { FinalItemGenerator(it.toString()) }

    private val step1Scheduler = scheduler(step1ThreadCoreSize, 32, "step1-")
    private val step2Scheduler = scheduler(step2ThreadCoreSize, 32, "step2-")
    private val finalSchedulers = (0 until topicSubscriberCount)
        .map { scheduler(finalItemThreadCoreSize, 32, "final-$it-") }

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
        val step2ItemSource = Flux
            .create<StartItem>({ startItemPublishAsynchronously(it, publishItemCount) }, BUFFER)
            .flatMapSequential<Step1Item>(this::generateStep1Item, step1ThreadCoreSize, 1)
            .flatMapSequential<Step2Item>(this::generateStep2ItemAndPass2Processor, step2ThreadCoreSize, 1)
            .flatMap<Step2Item>(this::pass2Processor, step2ThreadCoreSize, 1)
            .doOnError(this::terminateOnUnrecoverableError)

        // step2 item publish & final transform
        (0 until topicSubscriberCount).forEach { index ->
            Flux
                .create<Step2Item>({ startStep2ItemPublishAsynchronously(processor, it) }, BUFFER)
                .filter { (it.value % topicSubscriberCount) == index }
                .flatMap({ generateFinalItem(it, index) }, finalItemThreadCoreSize, 1)
//                .log()
                .doOnError(this::terminateOnUnrecoverableError)
                .subscribe()
        }

        step2ItemSource.subscribe()

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

    private fun startStep2ItemPublishAsynchronously(processor: Processor, it: FluxSink<Step2Item>) {
        processor.register(object : Step2ItemListener {
            override fun onReceive(item: Step2Item) {
                it.next(item)
            }
        })
    }

    private fun generateStep1Item(source: StartItem): Mono<Step1Item> {
        return Mono.create<Step1Item> {
            step1MetricTimer.record {
                it.success(step1Generator.withDelayMillis(source, 1))
            }
        }.subscribeOn(step1Scheduler)
    }

    private fun generateStep2ItemAndPass2Processor(source: Step1Item): Mono<Step2Item> {
        return Mono.create<Step2Item> {
            step2MetricTimer.record {
                val step2Item = step2Generator.withDelayMillis(source, 5)
                processor.execute(step2Item)
                it.success(step2Item)
            }
        }.subscribeOn(step2Scheduler)
    }

    private fun pass2Processor(source: Step2Item): Mono<Step2Item> {
        return Mono.create<Step2Item> {
            it.success(processor.execute(source))
        }.subscribeOn(step2Scheduler)
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

    internal class Processor {

        private val step2ItemListeners = mutableListOf<Step2ItemListener>()

        fun register(step2ItemListener: Step2ItemListener) {
            step2ItemListeners.add(step2ItemListener)
        }

        fun execute(step2Item: Step2Item): Step2Item {
            step2ItemListeners.forEach { it.onReceive(step2Item) }
            return step2Item
        }
    }

    internal interface Step2ItemListener {

        fun onReceive(item: Step2Item)

    }
}
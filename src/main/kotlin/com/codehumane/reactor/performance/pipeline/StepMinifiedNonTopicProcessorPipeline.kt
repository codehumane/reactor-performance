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
start tps: 11658.444444444445, detail: 11960, 10358, 10071, 12070, 12200, 12111, 11865, 12196, 12095
step1 tps: 7108.666666666667, detail: 7137, 7119, 7100, 7109, 7054, 7172, 7117, 7081, 7089
step2 tps: 7107.555555555556, detail: 7139, 7115, 7099, 7112, 7058, 7160, 7127, 7073, 7085
pipeline_final_0 tps: 444.3333333333333, detail: 447, 445, 443, 443, 441, 449, 446, 442, 443
pipeline_final_1 tps: 444.1111111111111, detail: 444, 445, 444, 445, 441, 449, 443, 443, 443
pipeline_final_2 tps: 444.22222222222223, detail: 446, 444, 444, 445, 442, 447, 446, 441, 443
pipeline_final_3 tps: 444.3333333333333, detail: 447, 445, 443, 444, 441, 448, 445, 445, 441
pipeline_final_4 tps: 444.22222222222223, detail: 445, 446, 443, 445, 440, 449, 444, 443, 443
pipeline_final_5 tps: 444.1111111111111, detail: 445, 445, 444, 444, 440, 448, 445, 443, 443
pipeline_final_6 tps: 444.3333333333333, detail: 448, 444, 443, 445, 441, 445, 448, 442, 443
pipeline_final_7 tps: 443.8888888888889, detail: 446, 445, 444, 443, 442, 446, 447, 440, 442
pipeline_final_8 tps: 444.44444444444446, detail: 447, 445, 444, 444, 441, 447, 445, 445, 442
pipeline_final_9 tps: 444.0, detail: 447, 444, 443, 442, 443, 447, 446, 442, 442
pipeline_final_10 tps: 444.3333333333333, detail: 445, 447, 443, 446, 441, 447, 446, 442, 442
pipeline_final_11 tps: 444.22222222222223, detail: 445, 446, 445, 443, 441, 448, 443, 446, 441
pipeline_final_12 tps: 444.1111111111111, detail: 446, 444, 445, 443, 440, 448, 445, 443, 443
pipeline_final_13 tps: 444.3333333333333, detail: 446, 444, 444, 445, 440, 448, 446, 443, 443
pipeline_final_14 tps: 444.22222222222223, detail: 448, 443, 444, 444, 442, 447, 447, 442, 441
pipeline_final_15 tps: 444.6666666666667, detail: 448, 445, 444, 444, 439, 451, 444, 441, 446

<거의 끝날 무렵>
start tps: 10833.444444444445, detail: 12061, 11711, 11804, 12095, 11776, 11579, 7350, 9310, 9815
step1 tps: 7001.888888888889, detail: 7084, 7045, 7078, 7133, 7043, 7199, 6499, 6826, 7110
step2 tps: 7002.222222222223, detail: 7092, 7043, 7076, 7128, 7043, 7210, 6504, 6810, 7114
pipeline_final_0 tps: 437.77777777777777, detail: 444, 440, 440, 446, 443, 448, 408, 426, 445
pipeline_final_1 tps: 437.55555555555554, detail: 442, 441, 443, 446, 439, 448, 408, 426, 445
pipeline_final_2 tps: 437.44444444444446, detail: 440, 442, 440, 446, 441, 452, 406, 425, 445
pipeline_final_3 tps: 437.77777777777777, detail: 444, 439, 442, 446, 440, 448, 411, 422, 448
pipeline_final_4 tps: 437.55555555555554, detail: 444, 440, 442, 444, 440, 452, 406, 426, 444
pipeline_final_5 tps: 437.6666666666667, detail: 444, 438, 443, 447, 439, 450, 407, 425, 446
pipeline_final_6 tps: 437.44444444444446, detail: 443, 439, 441, 447, 441, 451, 405, 425, 445
pipeline_final_7 tps: 437.6666666666667, detail: 442, 441, 443, 446, 440, 450, 406, 425, 446
pipeline_final_8 tps: 437.77777777777777, detail: 444, 441, 441, 447, 438, 452, 406, 426, 445
pipeline_final_9 tps: 437.77777777777777, detail: 443, 439, 442, 445, 442, 449, 408, 427, 445
pipeline_final_10 tps: 437.8888888888889, detail: 445, 439, 442, 446, 439, 452, 407, 425, 446
pipeline_final_11 tps: 437.6666666666667, detail: 445, 440, 440, 448, 440, 450, 408, 425, 443
pipeline_final_12 tps: 437.77777777777777, detail: 443, 439, 442, 447, 440, 450, 407, 426, 446
pipeline_final_13 tps: 437.8888888888889, detail: 445, 440, 441, 444, 441, 452, 405, 428, 445
pipeline_final_14 tps: 437.44444444444446, detail: 444, 438, 444, 445, 441, 450, 407, 426, 442
pipeline_final_15 tps: 437.3333333333333, detail: 443, 441, 440, 447, 439, 450, 407, 427, 442
 */
@Service
class StepMinifiedNonTopicProcessorPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(StepMinifiedNonTopicProcessorPipeline::class.java)

    private val intermediateTransformThreadCoreSize = 32
    private val finalItemThreadCoreSize = 4
    private val topicSubscriberCount = 16
    private val processor = Processor()

    private val itemGenerator = StartItemGenerator()
    private val step1Generator = Step1ItemGenerator()
    private val step2Generator = Step2ItemGenerator()
    private val finalGenerators = (0 until topicSubscriberCount)
        .map { FinalItemGenerator(it.toString()) }

    private val intermediateTransformScheduler = scheduler(intermediateTransformThreadCoreSize, 32, "step2-")
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
            .flatMapSequential<Step2Item>(this::transformItemAndPassToProcessor, intermediateTransformThreadCoreSize, 1)
            .doOnError(this::terminateOnUnrecoverableError)

        // step2 item publish & final transform
        (0 until topicSubscriberCount).forEach { index ->
            Flux
                .create<Step2Item>({ startStep2ItemPublishAsynchronously(processor, it) }, BUFFER)
                .filter { (it.value % topicSubscriberCount) == index }
                .flatMap({ generateFinalItem(it, index) }, finalItemThreadCoreSize, 1)
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

    private fun transformItemAndPassToProcessor(source: StartItem): Mono<Step2Item> {
        return Mono.create<Step2Item> {

            val step1Item = step1MetricTimer.record<Step1Item> {
                step1Generator.withDelayMillis(source, 1)
            }

            step2MetricTimer.record {
                val step2Item = step2Generator.withDelayMillis(step1Item, 5)
                processor.execute(step2Item)
                it.success(step2Item)
            }

        }.subscribeOn(intermediateTransformScheduler)

    }

    private fun pass2Processor(source: Step2Item): Mono<Step2Item> {
        return Mono.create<Step2Item> {
            it.success(processor.execute(source))
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
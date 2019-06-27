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
 * <EfficientStepMinifiedNonTopicProcessorPipeline에서 바꾼 것은>
 * - topic processor 사용하지 않고,
 * - 직접 fan out 시키는 processor 만들어서 사용한 방식이며,
 * - 1단계 2단계를 통합한 뒤,
 * - fan out 시 불필요한 sink#next를 제거한 버전 (sink#next 뒤에서 하던 filter를 sink#next 앞으로 뺀 것)
 *
 * <결론>
 * sink#next 자체의 비용은 거의 없다!!
 *
<시작하고 두 20초 뒤>
start tps: 11269.555555555555, detail: 11805, 11403, 11314, 11207, 10447, 11029, 11641, 11563, 11017
step1 tps: 7122.0, detail: 7166, 7056, 7110, 7088, 7149, 7115, 7139, 7136, 7139
step2 tps: 7121.111111111111, detail: 7171, 7056, 7096, 7096, 7140, 7120, 7143, 7132, 7136
pipeline_final_0 tps: 444.8888888888889, detail: 446, 442, 443, 444, 446, 445, 448, 444, 446
pipeline_final_1 tps: 445.0, detail: 448, 441, 444, 443, 448, 443, 447, 444, 447
pipeline_final_2 tps: 445.1111111111111, detail: 447, 443, 443, 443, 447, 446, 444, 446, 447
pipeline_final_3 tps: 444.77777777777777, detail: 447, 442, 441, 444, 448, 443, 446, 448, 444
pipeline_final_4 tps: 444.8888888888889, detail: 447, 441, 445, 444, 444, 445, 445, 446, 447
pipeline_final_5 tps: 445.1111111111111, detail: 448, 441, 444, 445, 444, 447, 447, 444, 446
pipeline_final_6 tps: 445.0, detail: 448, 440, 444, 444, 448, 442, 448, 446, 445
pipeline_final_7 tps: 445.0, detail: 449, 441, 445, 441, 446, 446, 447, 444, 446
pipeline_final_8 tps: 444.8888888888889, detail: 449, 440, 444, 443, 447, 444, 447, 445, 445
pipeline_final_9 tps: 445.1111111111111, detail: 447, 442, 442, 447, 446, 445, 446, 445, 446
pipeline_final_10 tps: 445.0, detail: 449, 442, 442, 443, 448, 444, 447, 445, 445
pipeline_final_11 tps: 445.0, detail: 446, 444, 444, 441, 448, 444, 446, 446, 446
pipeline_final_12 tps: 444.8888888888889, detail: 448, 440, 443, 445, 445, 444, 449, 445, 445
pipeline_final_13 tps: 445.1111111111111, detail: 450, 441, 442, 443, 447, 445, 447, 445, 446
pipeline_final_14 tps: 445.0, detail: 449, 442, 444, 442, 446, 445, 447, 445, 445
pipeline_final_15 tps: 445.0, detail: 449, 441, 442, 443, 446, 446, 448, 445, 445

<거의 끝날 무렵>
start tps: 11045.333333333334, detail: 11644, 11738, 11656, 11219, 8352, 10627, 11414, 11484, 11274
step1 tps: 7054.555555555556, detail: 7074, 7133, 7155, 7035, 6692, 7145, 7077, 7111, 7069
step2 tps: 7054.666666666667, detail: 7067, 7146, 7154, 7046, 6675, 7153, 7079, 7111, 7061
pipeline_final_0 tps: 441.1111111111111, detail: 441, 447, 447, 442, 417, 446, 443, 444, 443
pipeline_final_1 tps: 440.8888888888889, detail: 442, 447, 448, 440, 418, 446, 441, 446, 440
pipeline_final_2 tps: 440.55555555555554, detail: 442, 444, 447, 442, 418, 446, 442, 445, 439
pipeline_final_3 tps: 440.77777777777777, detail: 441, 444, 449, 440, 417, 447, 442, 445, 442
pipeline_final_4 tps: 440.77777777777777, detail: 441, 447, 449, 439, 417, 446, 443, 443, 442
pipeline_final_5 tps: 441.0, detail: 443, 445, 448, 440, 417, 447, 443, 444, 442
pipeline_final_6 tps: 441.1111111111111, detail: 442, 448, 448, 440, 416, 447, 443, 444, 442
pipeline_final_7 tps: 441.22222222222223, detail: 442, 446, 448, 439, 419, 446, 442, 445, 444
pipeline_final_8 tps: 440.8888888888889, detail: 440, 449, 446, 442, 416, 449, 438, 445, 443
pipeline_final_9 tps: 441.22222222222223, detail: 444, 444, 450, 440, 417, 448, 441, 443, 444
pipeline_final_10 tps: 440.8888888888889, detail: 440, 448, 448, 441, 416, 448, 441, 443, 443
pipeline_final_11 tps: 440.6666666666667, detail: 440, 449, 448, 439, 417, 446, 444, 443, 440
pipeline_final_12 tps: 440.77777777777777, detail: 443, 445, 449, 439, 416, 447, 444, 443, 441
pipeline_final_13 tps: 440.77777777777777, detail: 439, 448, 447, 440, 418, 448, 440, 445, 442
pipeline_final_14 tps: 440.8888888888889, detail: 443, 447, 446, 442, 414, 448, 444, 444, 440
pipeline_final_15 tps: 440.55555555555554, detail: 442, 444, 448, 440, 418, 445, 443, 445, 440
 */
@Service
class EfficientStepMinifiedNonTopicProcessorPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(EfficientStepMinifiedNonTopicProcessorPipeline::class.java)

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
                .create<Step2Item>({ startStep2ItemPublishAsynchronously(processor, index, it) }, BUFFER)
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

    private fun startStep2ItemPublishAsynchronously(processor: Processor, index: Int, sink: FluxSink<Step2Item>) {
        processor.register(object : Step2ItemListener {
            override fun onReceive(item: Step2Item) {
                if (item.value % topicSubscriberCount == index) {
                    sink.next(item)
                }
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
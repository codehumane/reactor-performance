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
 * `StepMinifiedNonTopicProcessorPipeline`와 기본적으로는 동일한데,
 * fan out 시 이벤트의 순서를 보장(binary log event 순서대로 하나씩 fan out)한다.
 * 순서 보장이라는 제약이 주어지면 성능은 얼마나 줄어들까?
 * (중간 변환 작업의 스레드는 128개로 늘려둔 상태에서 성능 측정함)
 *
 * <결론>
 * `StepMinifiedNonTopicProcessorPipeline`와 성능 거의 유사 (오히려 좀 더 좋은 성능을 보임?!)
 * 단순히 `sink#next`의 호출이 부담을 주는 것은 아님.
 * 병렬로 처리하던 것을 하나의 스레드에서 수행하더라도 차이가 없을 정도로 말이다.
 * 오히려 순서 보장이라는 더 큰 이득을 가져다 줌.
 * 일반적으로는 이 구현이 성능과 순서 보장 두 측면을 모두 고려할 때, 제일 좋은 선택으로 보인다.
 *
<시작하고 두 20초 뒤>
start tps: 8054.111111111111, detail: 7579, 8173, 8203, 8231, 8235, 8242, 7898, 7893, 8033
step1 tps: 8054.111111111111, detail: 7579, 8173, 8203, 8231, 8235, 8242, 7898, 7893, 8033
step2 tps: 8054.222222222223, detail: 7585, 8176, 8198, 8233, 8234, 8236, 7902, 7889, 8035
pipeline_final_0 tps: 503.6666666666667, detail: 475, 512, 512, 512, 516, 517, 494, 492, 503
pipeline_final_1 tps: 503.6666666666667, detail: 475, 510, 514, 512, 516, 517, 494, 491, 504
pipeline_final_2 tps: 503.44444444444446, detail: 473, 511, 514, 512, 516, 515, 497, 491, 502
pipeline_final_3 tps: 503.6666666666667, detail: 473, 512, 513, 514, 515, 515, 495, 492, 504
pipeline_final_4 tps: 503.44444444444446, detail: 473, 512, 513, 512, 516, 516, 495, 493, 501
pipeline_final_5 tps: 503.22222222222223, detail: 473, 511, 514, 515, 514, 514, 495, 493, 500
pipeline_final_6 tps: 503.1111111111111, detail: 473, 509, 511, 519, 515, 514, 496, 489, 502
pipeline_final_7 tps: 503.22222222222223, detail: 474, 512, 513, 512, 517, 515, 495, 492, 499
pipeline_final_8 tps: 503.3333333333333, detail: 475, 511, 512, 515, 516, 513, 495, 492, 501
pipeline_final_9 tps: 503.44444444444446, detail: 476, 509, 516, 510, 517, 516, 493, 493, 501
pipeline_final_10 tps: 503.3333333333333, detail: 473, 513, 511, 517, 512, 516, 496, 490, 502
pipeline_final_11 tps: 503.3333333333333, detail: 475, 511, 511, 515, 515, 515, 493, 493, 502
pipeline_final_12 tps: 503.44444444444446, detail: 475, 511, 511, 516, 514, 514, 496, 492, 502
pipeline_final_13 tps: 503.55555555555554, detail: 473, 512, 512, 514, 516, 514, 496, 490, 505
pipeline_final_14 tps: 503.3333333333333, detail: 471, 513, 510, 514, 517, 516, 494, 493, 502
pipeline_final_15 tps: 503.1111111111111, detail: 473, 508, 517, 514, 514, 514, 492, 496, 500

<거의 끝날 무렵>
start tps: 8050.111111111111, detail: 8093, 8150, 8006, 8107, 7967, 7958, 8112, 8092, 7966
step1 tps: 8050.111111111111, detail: 8093, 8150, 8006, 8107, 7967, 7957, 8113, 8092, 7966
step2 tps: 8051.777777777777, detail: 8099, 8148, 7998, 8111, 7966, 7961, 8112, 8093, 7978
pipeline_final_0 tps: 503.3333333333333, detail: 508, 508, 501, 507, 499, 493, 511, 504, 499
pipeline_final_1 tps: 503.22222222222223, detail: 505, 508, 502, 506, 499, 495, 511, 504, 499
pipeline_final_2 tps: 503.44444444444446, detail: 509, 505, 504, 506, 497, 496, 509, 504, 501
pipeline_final_3 tps: 503.6666666666667, detail: 509, 506, 502, 508, 499, 495, 507, 506, 501
pipeline_final_4 tps: 503.3333333333333, detail: 506, 508, 502, 509, 494, 497, 510, 504, 500
pipeline_final_5 tps: 503.3333333333333, detail: 504, 512, 500, 506, 498, 496, 511, 504, 499
pipeline_final_6 tps: 503.44444444444446, detail: 506, 508, 502, 505, 500, 496, 509, 505, 500
pipeline_final_7 tps: 503.44444444444446, detail: 504, 511, 500, 507, 499, 496, 510, 502, 502
pipeline_final_8 tps: 503.44444444444446, detail: 508, 508, 500, 506, 500, 495, 509, 504, 501
pipeline_final_9 tps: 503.22222222222223, detail: 505, 507, 503, 505, 499, 497, 508, 503, 502
pipeline_final_10 tps: 503.55555555555554, detail: 508, 506, 504, 507, 499, 495, 509, 502, 502
pipeline_final_11 tps: 503.3333333333333, detail: 506, 506, 505, 504, 499, 495, 511, 503, 501
pipeline_final_12 tps: 503.44444444444446, detail: 509, 508, 500, 507, 499, 497, 507, 505, 499
pipeline_final_13 tps: 503.22222222222223, detail: 506, 509, 500, 506, 498, 495, 512, 504, 499
pipeline_final_14 tps: 503.1111111111111, detail: 505, 509, 501, 506, 500, 496, 507, 505, 499
pipeline_final_15 tps: 503.3333333333333, detail: 507, 507, 501, 509, 496, 497, 510, 504, 499
 */
@Service
class OrderingStepMinifiedNonTopicProcessorPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(OrderingStepMinifiedNonTopicProcessorPipeline::class.java)

    private val intermediateTransformThreadCoreSize = 128
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
            .flatMapSequential<Step2Item>(this::transform, intermediateTransformThreadCoreSize, 1)
            .map(processor::execute)
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
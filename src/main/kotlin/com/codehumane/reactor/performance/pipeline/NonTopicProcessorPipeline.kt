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
 * - topic processor가 느리다고 판단하여 직접 (fan out) processor 작성한 버전
 *
<1,000,000건 실행 후, 시작하고 두 20초 뒤>
start tps: 11728.0, detail: 11733, 11948, 11431, 11331, 11933, 11932, 11808, 11780, 11656
step1 tps: 6770.444444444444, detail: 6655, 6998, 6754, 6952, 6661, 6790, 6486, 6711, 6927
step2 tps: 6770.333333333333, detail: 6646, 7002, 6750, 6954, 6657, 6788, 6492, 6713, 6931
pipeline_final_0 tps: 423.0, detail: 414, 438, 421, 435, 417, 425, 404, 421, 432
pipeline_final_1 tps: 423.3333333333333, detail: 416, 439, 422, 433, 417, 424, 403, 422, 434
pipeline_final_2 tps: 423.0, detail: 416, 437, 422, 434, 417, 424, 407, 418, 432
pipeline_final_3 tps: 423.1111111111111, detail: 414, 438, 422, 434, 415, 425, 405, 423, 432
pipeline_final_4 tps: 423.22222222222223, detail: 416, 437, 423, 434, 417, 425, 406, 419, 432
pipeline_final_5 tps: 423.0, detail: 416, 435, 423, 436, 415, 424, 407, 417, 434
pipeline_final_6 tps: 423.0, detail: 414, 437, 422, 435, 415, 425, 404, 424, 431
pipeline_final_7 tps: 423.1111111111111, detail: 416, 437, 423, 433, 416, 424, 406, 423, 430
pipeline_final_8 tps: 423.3333333333333, detail: 417, 437, 422, 434, 416, 424, 407, 419, 434
pipeline_final_9 tps: 423.1111111111111, detail: 414, 438, 421, 435, 417, 423, 407, 418, 435
pipeline_final_10 tps: 423.22222222222223, detail: 415, 438, 420, 435, 417, 425, 405, 420, 434
pipeline_final_11 tps: 423.22222222222223, detail: 415, 437, 423, 435, 417, 423, 407, 419, 433
pipeline_final_12 tps: 423.22222222222223, detail: 415, 438, 421, 436, 416, 423, 407, 421, 432
pipeline_final_13 tps: 423.22222222222223, detail: 416, 435, 423, 436, 415, 427, 405, 420, 432
pipeline_final_14 tps: 423.0, detail: 414, 438, 422, 435, 414, 424, 408, 419, 433
pipeline_final_15 tps: 423.22222222222223, detail: 416, 438, 422, 434, 416, 425, 403, 422, 433

<1,000,000건 실행 후, 거의 끝날 무렵>
start tps: 9179.666666666666, detail: 9978, 8054, 10069, 9940, 10942, 9049, 10132, 8614, 5839
step1 tps: 6187.111111111111, detail: 6531, 6509, 6271, 6645, 5979, 5672, 6300, 6364, 5413
step2 tps: 6187.444444444444, detail: 6540, 6512, 6264, 6642, 5981, 5669, 6297, 6398, 5384
pipeline_final_0 tps: 386.55555555555554, detail: 410, 408, 391, 415, 372, 355, 395, 400, 333
pipeline_final_1 tps: 386.6666666666667, detail: 408, 409, 389, 417, 374, 354, 395, 399, 335
pipeline_final_2 tps: 386.77777777777777, detail: 410, 405, 390, 418, 374, 353, 395, 400, 336
pipeline_final_3 tps: 386.77777777777777, detail: 410, 407, 391, 415, 374, 354, 394, 401, 335
pipeline_final_4 tps: 387.0, detail: 412, 407, 389, 417, 375, 352, 396, 399, 336
pipeline_final_5 tps: 387.0, detail: 410, 407, 391, 416, 376, 352, 393, 402, 336
pipeline_final_6 tps: 386.8888888888889, detail: 411, 408, 389, 417, 373, 354, 394, 401, 335
pipeline_final_7 tps: 386.77777777777777, detail: 410, 407, 390, 417, 374, 355, 393, 401, 334
pipeline_final_8 tps: 386.8888888888889, detail: 409, 407, 394, 414, 373, 353, 396, 400, 336
pipeline_final_9 tps: 386.6666666666667, detail: 411, 405, 392, 415, 374, 355, 392, 401, 335
pipeline_final_10 tps: 386.77777777777777, detail: 410, 406, 391, 416, 375, 354, 392, 402, 335
pipeline_final_11 tps: 386.8888888888889, detail: 409, 409, 391, 416, 372, 355, 392, 403, 335
pipeline_final_12 tps: 386.55555555555554, detail: 407, 409, 390, 414, 376, 353, 394, 401, 335
pipeline_final_13 tps: 386.77777777777777, detail: 408, 409, 392, 415, 373, 355, 393, 402, 334
pipeline_final_14 tps: 387.1111111111111, detail: 410, 408, 390, 416, 373, 355, 393, 401, 338
pipeline_final_15 tps: 386.8888888888889, detail: 410, 408, 390, 417, 372, 354, 395, 401, 335
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
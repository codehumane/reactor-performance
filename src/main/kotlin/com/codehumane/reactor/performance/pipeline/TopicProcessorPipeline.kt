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
import reactor.core.publisher.TopicProcessor
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.concurrent.WaitStrategy
import java.util.concurrent.CompletableFuture
import kotlin.math.pow
import kotlin.system.exitProcess

/**
 * binary log event를 받아서 replicate item으로 변환하고 각 샤드 DB로 복제하는 일련의 파이프라인을 구성
 */
@Service
class TopicProcessorPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(TopicProcessorPipeline::class.java)

    private val step1ThreadCoreSize = 32
    private val step2ThreadCoreSize = 32
    private val finalItemThreadCoreSize = 4
    private val topicSubscriberCount = 16

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

    init {
        startTpsCollector()
    }

    /**
     * 파이프라인 실행 (구독)
     */
    fun start(publishItemCount: Int) {

        // topic prepare
        val topicProcessor = TopicProcessor
            .builder<Step2Item>()
            .name("final-topic")
            .bufferSize(2.toDouble().pow(13.toDouble()).toInt())
            .waitStrategy(WaitStrategy.busySpin())
            .build()

        // topic publish & intermediate transform
        Flux.create<StartItem>({ startItemPublishAsynchronously(it, publishItemCount) }, BUFFER)
            .flatMapSequential<Step1Item>({ generateStep1Item(it) }, step1ThreadCoreSize, 1)
            .flatMapSequential<Step2Item>({ generateStep2Item(it) }, step2ThreadCoreSize, 1)
            .doOnError { terminateOnUnrecoverableError(it) }
            .subscribe(topicProcessor)
//            .subscribe()

        // topic subscription & final transform
        (0 until topicSubscriberCount).forEach { index ->
            Flux.from(topicProcessor)
                .filter { (it.value % topicSubscriberCount) == index }
                .flatMap({ generateFinalItem(it, index) }, finalItemThreadCoreSize, 1)
                .doOnError { terminateOnUnrecoverableError(it) }
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

    private fun generateStep1Item(source: StartItem): Mono<Step1Item> {
        return Mono.create<Step1Item> {
            step1MetricTimer.record {
                it.success(step1Generator.withDelayMillis(source, 1))
            }
        }.subscribeOn(step1Scheduler)
    }

    private fun generateStep2Item(source: Step1Item): Mono<Step2Item> {
        return Mono.create<Step2Item> {
            step2MetricTimer.record {
                it.success(step2Generator.withDelayMillis(source, 5))
            }
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

}
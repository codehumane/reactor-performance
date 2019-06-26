package com.codehumane.reactor.performance.pipeline

import com.codehumane.reactor.performance.item.*
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
import java.util.concurrent.CompletableFuture
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

    val itemGenerator = StartItemGenerator()
    val step1Generator = Step1ItemGenerator()
    val step2Generator = Step2ItemGenerator()
    val finalGenerators = (0 until topicSubscriberCount)
        .map { FinalItemGenerator(it.toString()) }

    val step1Scheduler = scheduler(step1ThreadCoreSize, 32, "step1-")
    val step2Scheduler = scheduler(step2ThreadCoreSize, 32, "step2-")
    val finalSchedulers = (0 until topicSubscriberCount)
        .map { scheduler(finalItemThreadCoreSize, 32, "final-$it-") }

    val startMetricTimer = meterRegistry.timer("pipeline_start")
    val step1MetricTimer = meterRegistry.timer("pipeline_step1")
    val step2MetricTimer = meterRegistry.timer("pipeline_step2")
    val finalMetricTimers = (0 until topicSubscriberCount)
        .map { meterRegistry.timer("pipeline_final_$it") }

    /**
     * 파이프라인 실행 (구독)
     */
    fun start(publishItemCount: Int) {

        // topic prepare
        val topicProcessor = TopicProcessor.create<Step2Item>("final-topic", topicSubscriberCount)

        // topic publish & intermediate transform
        Flux.create<StartItem>({ startItemPublishAsynchronously(it, publishItemCount) }, BUFFER)
            .flatMapSequential<Step1Item>({ generateStep1Item(it) }, step1ThreadCoreSize, 1)
            .flatMapSequential<Step2Item>({ generateStep2Item(it) }, step2ThreadCoreSize, 1)
            .doOnError { terminateOnUnrecoverableError(it) }
            .subscribe(topicProcessor)

        // topic subscription & final transform
        (0 until topicSubscriberCount).forEach { index ->
            Flux.from(topicProcessor)
                .filter { (it.value % topicSubscriberCount) == index }
                .flatMap({ generateFinalItem(it, index) }, finalItemThreadCoreSize, 1)
                .log()
                .doOnError { terminateOnUnrecoverableError(it) }
                .subscribe()
        }

    }

    private fun scheduler(corePoolSize: Int, queueCapacity: Int, namePrefix: String): Scheduler {
        val executor = ThreadPoolTaskExecutor().apply {
            this.corePoolSize = corePoolSize
            setQueueCapacity(queueCapacity)
            setThreadNamePrefix(namePrefix)
            initialize()
        }

        return Schedulers.fromExecutorService(executor.threadPoolExecutor)
    }

    private fun startItemPublishAsynchronously(sink: FluxSink<StartItem>, count: Int) {
        log.info("play ground publishing source")
        CompletableFuture.runAsync {

            (0 until count).forEach { index ->
                startMetricTimer.record {
                    sink.next(itemGenerator.withDelay())
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
                it.success(step1Generator.withDelay(source))
            }
        }.subscribeOn(step1Scheduler)
    }

    private fun generateStep2Item(source: Step1Item): Mono<Step2Item> {
        return Mono.create<Step2Item> {
            step2MetricTimer.record {
                it.success(step2Generator.withDelay(source))
            }
        }.subscribeOn(step2Scheduler)
    }

    private fun generateFinalItem(source: Step2Item, index: Int): Mono<FinalItem> {
        val generator = finalGenerators[index]
        val scheduler = finalSchedulers[index]
        val timer = finalMetricTimers[index]

        return Mono.create<FinalItem> {
            timer.record {
                it.success(generator.withDelay(source))
            }
        }.subscribeOn(scheduler)
    }


    /**
     * 복구 불가능한 오류가 발생한 경우 시스템을 종료한다.
     */
    private fun terminateOnUnrecoverableError(it: Throwable?) {
        log.error("unrecoverable error. system exit", it)
        exitProcess(666)
    }

}
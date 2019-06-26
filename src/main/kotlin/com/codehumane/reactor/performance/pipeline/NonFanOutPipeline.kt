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
 * topic processor도 사용하지 않고,
 * fan out도 하지 않는 버전.
 *
 * final metric 값 혼란 주의
 *

<20초 뒤 성능>
start tps: 11234.777777777777, detail: 11252, 10931, 11090, 11108, 11413, 11671, 10816, 11146, 11686
step1 tps: 5199.333333333333, detail: 5568, 5061, 5556, 5153, 5622, 5414, 4587, 4812, 5021
step2 tps: 5198.666666666667, detail: 5570, 5059, 5553, 5150, 5625, 5418, 4585, 4812, 5016
final tps: 5199.111111111111, detail: 5579, 5050, 5557, 5149, 5627, 5413, 4599, 4812, 5006

<끝날 무렵>
start tps: 11333.0, detail: 11245, 11087, 11597, 11410, 11241, 11315, 11541, 11483, 11078
step1 tps: 4969.222222222223, detail: 4119, 4819, 4980, 5039, 5545, 4877, 5215, 5338, 4791
step2 tps: 4969.222222222223, detail: 4118, 4823, 4979, 5030, 5559, 4872, 5211, 5344, 4787
final tps: 4969.444444444444, detail: 4114, 4823, 4973, 5038, 5555, 4876, 5203, 5353, 4790
 */
@Service
class NonFanOutPipeline(meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(TopicProcessorPipeline::class.java)

    private val step1ThreadCoreSize = 32
    private val step2ThreadCoreSize = 32
    private val finalItemThreadCoreSize = 32
    private val itemGenerator = StartItemGenerator()
    private val step1Generator = Step1ItemGenerator()
    private val step2Generator = Step2ItemGenerator()
    private val finalGenerator = FinalItemGenerator("single-version")
    private val step1Scheduler = scheduler(step1ThreadCoreSize, 32, "step1-")
    private val step2Scheduler = scheduler(step2ThreadCoreSize, 32, "step2-")
    private val finalScheduler = scheduler(finalItemThreadCoreSize, 32, "final-single-")
    private val startMetricTimer = meterRegistry.timer("pipeline_start")
    private val step1MetricTimer = meterRegistry.timer("pipeline_step1")
    private val step2MetricTimer = meterRegistry.timer("pipeline_step2")
    private val finalMetricTimer = meterRegistry.timer("pipeline_final_single")

    /**
     * 파이프라인 실행 (구독)
     */
    fun start(publishItemCount: Int) {

        startTpsCollector()

        Flux
            .create<StartItem>({ startItemPublishAsynchronously(it, publishItemCount) }, BUFFER)
            .flatMapSequential<Step1Item>(this::generateStep1Item, step1ThreadCoreSize, 1)
            .flatMapSequential<Step2Item>(this::generateStep2Item, step2ThreadCoreSize, 1)
            .flatMap<FinalItem>(this::generateFinalItem, finalItemThreadCoreSize, 1)
            .doOnError(this::terminateOnUnrecoverableError)
            .subscribe()

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

    private fun generateFinalItem(source: Step2Item): Mono<FinalItem> {
        return Mono.create<FinalItem> {
            finalMetricTimer.record {
                it.success(finalGenerator.withDelayMillis(source, 10))
            }
        }.subscribeOn(finalScheduler)
    }

    private fun terminateOnUnrecoverableError(it: Throwable?) {
        log.error("unrecoverable error. system exit", it)
        exitProcess(666)
    }

    private fun startTpsCollector() {
        val tpsCollectorSource = mutableMapOf(
            "start" to startMetricTimer,
            "step1" to step1MetricTimer,
            "step2" to step2MetricTimer,
            "final" to finalMetricTimer
        )

        TPSCollector(10, tpsCollectorSource).start()
    }
}
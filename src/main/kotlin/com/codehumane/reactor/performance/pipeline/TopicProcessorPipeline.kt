package com.codehumane.reactor.performance.pipeline

import com.codehumane.reactor.performance.item.*
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration
import kotlin.system.exitProcess

/**
 * binary log event를 받아서 replicate item으로 변환하고 각 샤드 DB로 복제하는 일련의 파이프라인을 구성
 */
@Service
class TopicProcessorPipeline {

    private val log = LoggerFactory.getLogger(TopicProcessorPipeline::class.java)

    val step1ThreadCoreSize = 32
    val step2ThreadCoreSize = 32
    val finalItemThreadCoreSize = 4
    val topicSubscriberCount = 16

    /**
     * 파이프라인 실행 (구독)
     */
    fun start(publishItemCount: Int) {

        // 도구 준비
        val topicProcessor = TopicProcessor.create<Step2Item>()
        val step1Generator = Step1ItemGenerator(scheduler(step1ThreadCoreSize, 32, "step1-"))
        val step2Generator = Step2ItemGenerator(scheduler(step2ThreadCoreSize, 32, "step2-"))

        // 소스 제공
        val source = Flux
            .create<StartItem>(
                { publishItems(it, publishItemCount) },
                FluxSink.OverflowStrategy.BUFFER
            )

        // 중간 변환
        source
            .flatMapSequential<Step1Item>({ step1Generator.withDelay(it) }, step1ThreadCoreSize, 1)
            .flatMapSequential<Step2Item>({ step2Generator.withDelay(it) }, step2ThreadCoreSize, 1)
            .log()
            .doOnError { terminateOnUnrecoverableError(it) }
            .subscribe(topicProcessor)

        // 토픽 구독 & 최종 변환
        (1..topicSubscriberCount).forEach { idx ->

            val finalGenerator =
                FinalItemGenerator(scheduler(finalItemThreadCoreSize, 32, "final-"))

            Flux
                .from(topicProcessor)
                .filter { (it.value % topicSubscriberCount) + 1 == idx }
                .flatMap({ finalGenerator.withDelay(it) }, finalItemThreadCoreSize, 1)
                .doOnError { terminateOnUnrecoverableError(it) }
                .subscribe()
        }

    }

    private fun publishItems(sink: FluxSink<StartItem>, count: Int) {
        log.info("play ground publishing source")
        (0..count).forEach { idx ->

            Thread.sleep(1)

            Mono
                .just(StartItem(idx))
                .delayElement(Duration.ofMillis(10))
                .subscribe { sink.next(it) }
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

    /**
     * 복구 불가능한 오류가 발생한 경우 시스템을 종료한다.
     */
    private fun terminateOnUnrecoverableError(it: Throwable?) {
        log.error("unrecoverable error. system exit", it)
        exitProcess(666)
    }

}
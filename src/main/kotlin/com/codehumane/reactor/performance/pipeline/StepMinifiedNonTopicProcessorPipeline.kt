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
 * * <NonTopicProcessorPipeline에서 바꾼 것은>
 * - 1단계 2단계를 통합함
 *
 * <결론>
 * 미미하게나마 성능이 개선됨!
 *
<시작하고 두 20초 뒤>
start tps: 11666.0, detail: 11085, 11921, 11473, 11806, 11915, 11326, 11905, 11986, 11577
step1 tps: 7120.666666666667, detail: 7097, 7085, 7115, 7142, 7190, 7121, 7048, 7161, 7127
step2 tps: 7119.888888888889, detail: 7107, 7078, 7106, 7157, 7186, 7124, 7038, 7165, 7118
pipeline_final_0 tps: 445.0, detail: 444, 444, 442, 448, 449, 445, 441, 449, 443
pipeline_final_1 tps: 444.77777777777777, detail: 445, 441, 446, 446, 449, 445, 439, 448, 444
pipeline_final_2 tps: 445.0, detail: 446, 442, 445, 446, 449, 446, 440, 449, 442
pipeline_final_3 tps: 445.1111111111111, detail: 445, 442, 446, 446, 450, 445, 440, 447, 445
pipeline_final_4 tps: 445.1111111111111, detail: 446, 442, 443, 448, 450, 444, 442, 447, 444
pipeline_final_5 tps: 445.0, detail: 445, 442, 446, 447, 450, 444, 440, 447, 444
pipeline_final_6 tps: 445.1111111111111, detail: 443, 443, 445, 448, 448, 447, 439, 447, 446
pipeline_final_7 tps: 445.0, detail: 443, 445, 443, 447, 451, 442, 442, 447, 445
pipeline_final_8 tps: 445.1111111111111, detail: 446, 441, 445, 445, 450, 446, 438, 449, 446
pipeline_final_9 tps: 445.22222222222223, detail: 446, 443, 442, 448, 448, 448, 439, 448, 445
pipeline_final_10 tps: 445.0, detail: 445, 441, 446, 446, 450, 445, 440, 447, 445
pipeline_final_11 tps: 445.0, detail: 444, 442, 443, 448, 450, 444, 440, 447, 447
pipeline_final_12 tps: 445.22222222222223, detail: 447, 441, 445, 447, 449, 444, 442, 447, 445
pipeline_final_13 tps: 445.0, detail: 443, 443, 444, 446, 450, 445, 441, 449, 444
pipeline_final_14 tps: 444.8888888888889, detail: 445, 442, 443, 448, 449, 444, 441, 449, 443
pipeline_final_15 tps: 444.8888888888889, detail: 445, 443, 443, 448, 449, 445, 439, 448, 444

<거의 끝날 무렵>
start tps: 11641.888888888889, detail: 11908, 11847, 11532, 11386, 11380, 11305, 11811, 11825, 11783
step1 tps: 7111.333333333333, detail: 7167, 7095, 7104, 7084, 7094, 7162, 7083, 7155, 7058
step2 tps: 7111.333333333333, detail: 7157, 7103, 7105, 7082, 7100, 7146, 7087, 7149, 7073
pipeline_final_0 tps: 444.44444444444446, detail: 446, 444, 444, 445, 443, 448, 443, 444, 443
pipeline_final_1 tps: 444.22222222222223, detail: 446, 445, 443, 443, 446, 445, 442, 445, 443
pipeline_final_2 tps: 444.6666666666667, detail: 447, 446, 445, 442, 443, 446, 443, 447, 443
pipeline_final_3 tps: 444.6666666666667, detail: 446, 445, 445, 444, 444, 446, 441, 448, 443
pipeline_final_4 tps: 444.3333333333333, detail: 447, 442, 447, 441, 446, 444, 444, 445, 443
pipeline_final_5 tps: 444.44444444444446, detail: 448, 442, 443, 444, 445, 446, 444, 446, 442
pipeline_final_6 tps: 444.22222222222223, detail: 445, 444, 447, 441, 446, 445, 443, 445, 442
pipeline_final_7 tps: 444.55555555555554, detail: 449, 443, 444, 443, 444, 445, 443, 450, 440
pipeline_final_8 tps: 444.44444444444446, detail: 448, 444, 444, 442, 446, 445, 443, 447, 441
pipeline_final_9 tps: 444.77777777777777, detail: 447, 446, 443, 443, 445, 445, 443, 449, 442
pipeline_final_10 tps: 444.3333333333333, detail: 448, 444, 443, 443, 445, 444, 445, 446, 441
pipeline_final_11 tps: 444.44444444444446, detail: 446, 445, 443, 443, 444, 445, 441, 450, 443
pipeline_final_12 tps: 444.44444444444446, detail: 447, 443, 444, 442, 446, 445, 444, 446, 443
pipeline_final_13 tps: 444.44444444444446, detail: 448, 441, 446, 442, 445, 447, 442, 447, 442
pipeline_final_14 tps: 444.55555555555554, detail: 447, 445, 444, 444, 444, 446, 443, 447, 441
pipeline_final_15 tps: 444.6666666666667, detail: 449, 442, 447, 440, 446, 445, 442, 447, 444


 *******************
좀 더 개선해서 통합된 단계의 스레드 갯수를 늘리니 더 개선됨!!!
 *******************

<시작할 무렵>
start tps: 9550.333333333334, detail: 9305, 10316, 9733, 8891, 9304, 9396, 7698, 10871, 10439
step1 tps: 9550.333333333334, detail: 9304, 10317, 9733, 8891, 9304, 9396, 7698, 10871, 10439
step2 tps: 9550.111111111111, detail: 9314, 10308, 9741, 8887, 9301, 9413, 7677, 10867, 10443
pipeline_final_0 tps: 596.5555555555555, detail: 579, 642, 613, 555, 580, 592, 477, 679, 652
pipeline_final_1 tps: 596.7777777777778, detail: 582, 644, 613, 552, 581, 593, 472, 681, 653
pipeline_final_2 tps: 596.7777777777778, detail: 585, 637, 615, 553, 584, 590, 475, 681, 651
pipeline_final_3 tps: 596.3333333333334, detail: 588, 634, 618, 555, 576, 595, 477, 677, 647
pipeline_final_4 tps: 596.4444444444445, detail: 583, 642, 613, 555, 582, 590, 477, 678, 648
pipeline_final_5 tps: 596.7777777777778, detail: 584, 645, 608, 558, 580, 591, 475, 679, 651
pipeline_final_6 tps: 597.1111111111111, detail: 585, 643, 612, 555, 579, 592, 472, 684, 652
pipeline_final_7 tps: 596.7777777777778, detail: 583, 644, 610, 556, 579, 592, 477, 676, 654
pipeline_final_8 tps: 597.0, detail: 582, 639, 616, 554, 581, 591, 474, 680, 656
pipeline_final_9 tps: 597.3333333333334, detail: 586, 641, 614, 555, 580, 593, 476, 678, 653
pipeline_final_10 tps: 596.7777777777778, detail: 586, 642, 612, 554, 579, 593, 474, 680, 651
pipeline_final_11 tps: 596.8888888888889, detail: 583, 644, 612, 556, 581, 590, 477, 680, 649
pipeline_final_12 tps: 596.6666666666666, detail: 584, 642, 611, 557, 578, 593, 478, 674, 653
pipeline_final_13 tps: 596.7777777777778, detail: 583, 642, 611, 555, 581, 592, 474, 676, 657
pipeline_final_14 tps: 596.8888888888889, detail: 586, 642, 611, 554, 582, 590, 479, 678, 650
pipeline_final_15 tps: 597.1111111111111, detail: 583, 644, 609, 553, 583, 591, 476, 677, 658

<끝날 무렵 = 사실은 두 번째 만에 끝남>
tart tps: 10407.444444444445, detail: 10231, 10441, 10310, 10651, 10417, 10512, 10416, 10316, 10373
step1 tps: 10407.444444444445, detail: 10230, 10442, 10310, 10651, 10417, 10512, 10416, 10316, 10373
step2 tps: 10407.777777777777, detail: 10235, 10436, 10310, 10648, 10419, 10518, 10415, 10313, 10376
pipeline_final_0 tps: 650.8888888888889, detail: 645, 648, 644, 669, 649, 656, 650, 649, 648
pipeline_final_1 tps: 650.2222222222222, detail: 634, 652, 650, 661, 654, 656, 654, 644, 647
pipeline_final_2 tps: 650.5555555555555, detail: 642, 650, 649, 662, 652, 658, 652, 645, 645
pipeline_final_3 tps: 650.1111111111111, detail: 639, 651, 646, 662, 655, 657, 650, 647, 644
pipeline_final_4 tps: 650.4444444444445, detail: 642, 653, 644, 666, 650, 655, 652, 645, 647
pipeline_final_5 tps: 651.0, detail: 642, 653, 647, 662, 652, 659, 652, 640, 652
pipeline_final_6 tps: 650.4444444444445, detail: 641, 647, 651, 662, 655, 655, 652, 646, 645
pipeline_final_7 tps: 650.3333333333334, detail: 643, 650, 647, 664, 654, 654, 654, 645, 642
pipeline_final_8 tps: 650.1111111111111, detail: 637, 654, 646, 664, 653, 655, 653, 642, 647
pipeline_final_9 tps: 650.4444444444445, detail: 638, 653, 647, 662, 655, 655, 648, 650, 646
pipeline_final_10 tps: 650.3333333333334, detail: 639, 650, 650, 662, 651, 660, 650, 644, 647
pipeline_final_11 tps: 649.8888888888889, detail: 640, 648, 648, 666, 648, 661, 648, 648, 642
pipeline_final_12 tps: 650.5555555555555, detail: 640, 648, 651, 667, 650, 656, 648, 650, 645
pipeline_final_13 tps: 650.6666666666666, detail: 641, 653, 644, 665, 649, 660, 648, 649, 647
pipeline_final_14 tps: 651.5555555555555, detail: 650, 649, 647, 661, 656, 658, 648, 645, 650
pipeline_final_15 tps: 650.0, detail: 638, 651, 646, 667, 649, 658, 650, 644, 647
 */
@Service
class StepMinifiedNonTopicProcessorPipeline(private val meterRegistry: PrometheusMeterRegistry) {

    private val log = LoggerFactory.getLogger(StepMinifiedNonTopicProcessorPipeline::class.java)

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
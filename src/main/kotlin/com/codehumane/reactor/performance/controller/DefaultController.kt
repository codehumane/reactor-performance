package com.codehumane.reactor.performance.controller

import com.codehumane.reactor.performance.pipeline.*
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono


@RestController
class DefaultController(
    private val topicPipeline: TopicProcessorPipeline,
    private val nonTopicPipeline: NonTopicProcessorPipeline,
    private val stepMinifiedNonTopicProcessorPipeline: StepMinifiedNonTopicProcessorPipeline,
    private val efficientStepMinifiedNonTopicProcessorPipeline: EfficientStepMinifiedNonTopicProcessorPipeline,
    private val nonFanOutPipeline: NonFanOutPipeline
) {

    @GetMapping("/pipeline/topic/start")
    fun startTopicPipeline(@RequestParam("count") publishItemCount: Int): Mono<String> {
        topicPipeline.start(publishItemCount)
        return Mono.just("started")
    }

    @GetMapping("/pipeline/nontopic/start")
    fun startNonTopicPipeline(@RequestParam("count") publishItemCount: Int): Mono<String> {
        nonTopicPipeline.start(publishItemCount)
        return Mono.just("started")
    }

    @GetMapping("/pipeline/stepminifiednontopic/start")
    fun startNonTopicStepMinifiedPipeline(@RequestParam("count") publishItemCount: Int): Mono<String> {
        stepMinifiedNonTopicProcessorPipeline.start(publishItemCount)
        return Mono.just("started")
    }

    @GetMapping("/pipeline/efficientstepminifiednontopic/start")
    fun startEfficientNonTopicStepMinifiedPipeline(@RequestParam("count") publishItemCount: Int): Mono<String> {
        efficientStepMinifiedNonTopicProcessorPipeline.start(publishItemCount)
        return Mono.just("started")
    }

    @GetMapping("/pipeline/nonfanout/start")
    fun nonFanOutTopicPipeline(@RequestParam("count") publishItemCount: Int): Mono<String> {
        nonFanOutPipeline.start(publishItemCount)
        return Mono.just("started")
    }

}

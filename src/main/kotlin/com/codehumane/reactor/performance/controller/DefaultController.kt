package com.codehumane.reactor.performance.controller

import com.codehumane.reactor.performance.pipeline.NonTopicProcessorPipeline
import com.codehumane.reactor.performance.pipeline.TopicProcessorPipeline
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono


@RestController
class DefaultController(
    private val topicPipeline: TopicProcessorPipeline,
    private val nonTopicPipeline: NonTopicProcessorPipeline
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

}

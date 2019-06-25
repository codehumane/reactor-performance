package com.codehumane.reactor.performance.controller

import com.codehumane.reactor.performance.pipeline.TopicProcessorPipeline
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono


@RestController
class DefaultController(private val pipeline: TopicProcessorPipeline) {

    @GetMapping("/pipeline/start")
    fun startPipeline(@RequestParam("count") publishItemCount: Int): Mono<String> {
        pipeline.start(publishItemCount)
        return Mono.just("started")
    }
}
package com.codehumane.reactor.performance.pipeline

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest
class TopicProcessorPipelineTest {

    @Autowired
    lateinit var pipeline: TopicProcessorPipeline

    @Test
    fun start() {
        pipeline.start(1_000)
        Thread.sleep(3_000)
    }
}
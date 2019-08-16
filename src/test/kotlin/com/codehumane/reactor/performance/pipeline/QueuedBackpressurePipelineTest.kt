package com.codehumane.reactor.performance.pipeline

import org.junit.Test
import java.util.concurrent.TimeUnit

class QueuedBackpressurePipelineTest {

    @Test
    fun start() {

        // given
        val pipeline = QueuedBackpressurePipeline(10, 200)
        pipeline.start()

        // when
        (1..20).forEach {
            pipeline.add(it)
        }

        // then
        TimeUnit.SECONDS.sleep(5)
        // 출력을 기다릴 뿐
    }

}
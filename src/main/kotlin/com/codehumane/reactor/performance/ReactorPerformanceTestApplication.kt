package com.codehumane.reactor.performance

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReactorPerformanceTestApplication

fun main(args: Array<String>) {
	runApplication<ReactorPerformanceTestApplication>(*args)
}

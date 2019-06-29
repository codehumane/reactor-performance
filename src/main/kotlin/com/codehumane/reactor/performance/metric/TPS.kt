package com.codehumane.reactor.performance.metric

internal data class TPS(private val name: String, private val maxCollectCount: Int) {

    init {
        require(maxCollectCount >= 1)
    }

    private val collection = mutableListOf<Long>()

    fun add(count: Long) {
        collection.add(count)
        if (collection.size > maxCollectCount) collection.removeAt(0)
    }

    fun size(): Int {
        return collection.size
    }

    fun get(): List<Long> {
        return collection.toList()
    }

    fun describe(): String {
        val tps = mutableListOf<Long>()
        (1 until collection.size).forEach { tps.add(collection[it] - collection[it - 1]) }
        val average = tps.average().let { if (it.isNaN()) 0 else it }
        return "$name tps: $average, detail: ${tps.joinToString()}"
    }

}
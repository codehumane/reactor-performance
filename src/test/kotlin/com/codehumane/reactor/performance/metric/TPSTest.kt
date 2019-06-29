package com.codehumane.reactor.performance.metric

import org.junit.Assert.assertEquals
import org.junit.Test

class TPSTest {

    @Test(expected = IllegalArgumentException::class)
    fun `인스턴스 생성 시 maxCollectionSize 값이 1보다 작을 수 없다`() {

        // when
        TPS("name", 0)
    }

    @Test
    fun `add 처음 추가하면 당연히 갯수는 1개가 되어야 한다`() {

        // given
        val tps = TPS("this-is-name", 10)

        // when
        tps.add(3)

        // then
        assertEquals(1, tps.size())
    }

    @Test
    fun `add maxCollectionSize 값보다 커지면 맨 처음 쌓인 값이 제거되어야 한다`() {

        // given
        val tps = TPS("max collection size constraints", 2)

        // and
        tps.add(3)
        tps.add(4)

        // when
        tps.add(5)

        // then
        assertEquals(2, tps.get().size)
        assertEquals(4, tps.get()[0])
        assertEquals(5, tps.get()[1])
    }

    @Test
    fun describe() {

        // given
        val name = "Let's describe."
        val tps = TPS(name, 3)

        // and
        tps.add(3)
        tps.add(4)
        tps.add(6)

        // when
        val description = tps.describe()

        // then
        assertEquals("$name tps: 1.5, detail: 1, 2", description)
    }

    @Test
    fun `describe 데이터가 없으면 0이라고 계산`() {

        // given
        val name = "no data"
        val tps = TPS(name, 3)

        // when
        val description = tps.describe()

        // then
        assertEquals("$name tps: 0, detail: ", description)
    }

}
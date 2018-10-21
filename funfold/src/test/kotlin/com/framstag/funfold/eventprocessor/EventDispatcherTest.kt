package com.framstag.funfold.eventprocessor

import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Event
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class EventDispatcherTest {
    data class TestAggregate(val id: String) : Aggregate
    data class TestEvent(val data: String) : Event
    data class AnotherTestEvent(val data: String) : Event

    @Test
    fun testCreation() {
        EventDispatcher()
    }

    @Test
    fun testRegisterHandler() {
        val dispatcher = EventDispatcher()

        dispatcher.registerEventHandler(TestAggregate::class.java,TestEvent::class.java) { _ ->
        }
    }

    @Test
    fun testRegisteredHandlerCalled() {
        val dispatcher = EventDispatcher()
        var called = false

        dispatcher.registerEventHandler(TestAggregate::class.java,TestEvent::class.java) { _ ->
            called = true
        }

        dispatcher.dispatch(TestAggregate::class.java,TestEvent("test"))

        assertTrue(called)
    }

    @Test
    fun testDispatchEventForUnknownAggregate() {
        val dispatcher = EventDispatcher()

        // Should be just ignored
        dispatcher.dispatch(TestAggregate::class.java,TestEvent("test"))
    }

    @Test
    fun testDispatchUnknownEvent() {
        val dispatcher = EventDispatcher()

        dispatcher.registerEventHandler(TestAggregate::class.java,TestEvent::class.java) { _ ->
        }

        dispatcher.dispatch(TestAggregate::class.java,AnotherTestEvent("test"))
    }

    @Test
    fun testGetObservedAggregatesWithoutRegistration() {
        val dispatcher = EventDispatcher()

        val aggregates = dispatcher.getObservedAggregates()

        assertEquals(0,aggregates.size)
    }

    @Test
    fun testGetObservedAggregatesWithOneRegistration() {
        val dispatcher = EventDispatcher()

        dispatcher.registerEventHandler(TestAggregate::class.java,TestEvent::class.java) { _ ->
        }

        val aggregates : List<Class<out Aggregate>> = dispatcher.getObservedAggregates()
        val expected : List<Class<out Aggregate>> =listOf(TestAggregate::class.java)

        assertEquals(expected,aggregates)
    }
}
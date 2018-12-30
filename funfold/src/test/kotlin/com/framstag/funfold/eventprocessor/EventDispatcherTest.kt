package com.framstag.funfold.eventprocessor

import com.framstag.funfold.cqrs.Event
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class EventDispatcherTest {
    data class TestEvent(val data: String) : Event
    data class AnotherTestEvent(val data: String) : Event

    @Test
    fun testCreation() {
        EventDispatcher()
    }

    @Test
    fun testRegisterHandler() {
        val dispatcher = EventDispatcher()

        dispatcher.registerEventHandler(TestEvent::class.java) { _ ->
        }
    }

    @Test
    fun testRegisteredHandlerCalled() {
        val dispatcher = EventDispatcher()
        var called = false

        dispatcher.registerEventHandler(TestEvent::class.java) { _ ->
            called = true
        }

        dispatcher.dispatch(TestEvent("test"))

        assertTrue(called)
    }

    @Test
    fun testDispatchEventForUnknownAggregate() {
        val dispatcher = EventDispatcher()

        // Should be just ignored
        dispatcher.dispatch(TestEvent("test"))
    }

    @Test
    fun testDispatchUnknownEvent() {
        val dispatcher = EventDispatcher()

        dispatcher.registerEventHandler(TestEvent::class.java) { _ ->
        }

        dispatcher.dispatch(AnotherTestEvent("test"))
    }
}

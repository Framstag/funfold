package com.framstag.funfold.eventstore.impl

import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Event
import com.framstag.funfold.eventstore.ProducedEventData
import com.framstag.funfold.exception.ConcurrencyViolationException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class InMemoryEventStoreTest {
    data class TestAggregate(val id: String) : Aggregate
    data class TestEvent(val data: String) : Event

    data class AnotherTestAggregate(val id: String) : Aggregate
    data class AnotherTestEvent(val data: String) : Event

    @Test
    fun testCreation() {
        InMemoryEventStore()
    }

    @Test
    fun testStoreEvent() {
        val eventStore = InMemoryEventStore()
        val event = TestEvent("data")
        val producedEvent = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            event
        )

        eventStore.saveEvent(producedEvent)
    }

    @Test
    fun testStoreEventWrongStartVersion() {
        val eventStore = InMemoryEventStore()
        val producedEvent1 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            2,
            TestEvent("data")
        )

        assertThrows<ConcurrencyViolationException> {
            eventStore.saveEvent(producedEvent1)
        }
    }

    @Test
    fun testStoreEventDuplicateVersion() {
        val eventStore = InMemoryEventStore()
        val producedEvent1 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            TestEvent("data")
        )

        eventStore.saveEvent(producedEvent1)

        val producedEvent2 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            TestEvent("data")
        )

        assertThrows<ConcurrencyViolationException> {
            eventStore.saveEvent(producedEvent2)
        }
    }

    @Test
    fun testStoreEventVersionGap() {
        val eventStore = InMemoryEventStore()
        val producedEvent1 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            TestEvent("data")
        )

        eventStore.saveEvent(producedEvent1)

        val producedEvent2 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            3,
            TestEvent("data")
        )

        assertThrows<ConcurrencyViolationException> {
            eventStore.saveEvent(producedEvent2)
        }
    }

    @Test
    fun testStoredEventCanBeLoadedViaSerialId() {
        val eventStore = InMemoryEventStore()
        val event = TestEvent("data")
        val producedEvent = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            event
        )

        eventStore.saveEvent(producedEvent)

        val loadedEvents = eventStore.loadEvents(0,50)

        assertEquals(1, loadedEvents.size)
        assertEquals(0L, loadedEvents.first().serial)

        val loadedEvent = loadedEvents.first()

        assertEquals(event, loadedEvent.event)
        assertEquals(producedEvent.aggregateName, loadedEvent.aggregateName)
        assertEquals(producedEvent.aggregateId, loadedEvent.aggregateId)
        assertEquals(producedEvent.version, loadedEvent.version)
        assertNotNull(loadedEvent.hash)
    }

    @Test
    fun testStoredEventCanBeLoadedViaAggregateInstance() {
        val eventStore = InMemoryEventStore()
        val event = TestEvent("data")
        val producedEvent = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            event
        )

        eventStore.saveEvent(producedEvent)

        val loadedEvents = eventStore.loadEvents(TestAggregate::class.java, "1")

        assertEquals(1, loadedEvents.size)
        assertEquals(0L, loadedEvents.first().serial)

        val loadedEvent = loadedEvents.first()

        assertEquals(event, loadedEvent.event)
        assertEquals(producedEvent.aggregateName, loadedEvent.aggregateName)
        assertEquals(producedEvent.aggregateId, loadedEvent.aggregateId)
        assertEquals(producedEvent.version, loadedEvent.version)
        assertNotNull(loadedEvent.hash)
    }

    @Test
    fun testLoadViaAggregateInstanceFiltersOtherAggregateEvents() {
        val eventStore = InMemoryEventStore()

        val event1 = TestEvent("data")
        val producedEvent1 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            event1
        )

        eventStore.saveEvent(producedEvent1)

        val event2 = AnotherTestEvent("data")
        val producedEvent2 = ProducedEventData(
            AnotherTestAggregate::class.java.name,
            "2",
            1,
            event2
        )

        eventStore.saveEvent(producedEvent2)

        val loadedEvents = eventStore.loadEvents(TestAggregate::class.java, "1")

        assertEquals(1, loadedEvents.size)

        val loadedEvent = loadedEvents.first()

        assertEquals(event1, loadedEvent.event)
    }

    @Test
    fun testStoredEventCanBeLoadedViaAggregateAndSerial() {
        val eventStore = InMemoryEventStore()
        val event = TestEvent("data")
        val producedEvent = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            event
        )

        eventStore.saveEvent(producedEvent)

        val loadedEvents = eventStore.loadEvents(TestAggregate::class.java, 0L)

        assertEquals(1, loadedEvents.size)
        assertEquals(0L, loadedEvents.first().serial)

        val loadedEvent = loadedEvents.first()

        assertEquals(event, loadedEvent.event)
        assertEquals(producedEvent.aggregateName, loadedEvent.aggregateName)
        assertEquals(producedEvent.aggregateId, loadedEvent.aggregateId)
        assertEquals(producedEvent.version, loadedEvent.version)
        assertNotNull(loadedEvent.hash)
    }

    @Test
    fun testLoadViaAggregateAndSerialFiltersOtherAggregateEvents() {
        val eventStore = InMemoryEventStore()

        val event1 = TestEvent("data")
        val producedEvent1 = ProducedEventData(
            TestAggregate::class.java.name,
            "1",
            1,
            event1
        )

        eventStore.saveEvent(producedEvent1)

        val event2 = AnotherTestEvent("data")
        val producedEvent2 = ProducedEventData(
            AnotherTestAggregate::class.java.name,
            "2",
            1,
            event2
        )

        eventStore.saveEvent(producedEvent2)

        val loadedEvents = eventStore.loadEvents(TestAggregate::class.java, 0L)

        assertEquals(1, loadedEvents.size)

        val loadedEvent = loadedEvents.first()

        assertEquals(event1, loadedEvent.event)
    }
}
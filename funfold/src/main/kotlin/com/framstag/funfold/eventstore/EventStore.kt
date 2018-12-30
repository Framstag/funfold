package com.framstag.funfold.eventstore

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Event

/**
 * The event store allows storing and loading of events.
 *
 * The event store assumes the following guaranties (some implementation will actively check these constraints):
 *
 * All events:
 * * Every event gets a serial id
 * * The serial id of an events is increasing in the order of creation, creating a global sequence
 *   of events in creation order.
 * * There are thus no two events with the same serial id
 * * Due to technical constraints the sequence of serial ids may contain gaps.
 *
 * Aggregate events:
 * * If the event belongs to an aggregate, the combination of aggregate name, aggregate id and version
 *   is unique.
 * * The version of an aggregate is increasing with every new event.
 * * Some implementations also may enforce that versions are monotonically increasing.
 *
 * To store an aggregate you need to create a ProducedEventData object holding additional meta data
 * While loading one or more events they will be returned as StoredEventData, thus return meta data
 * again.
 *
 * TODO: I would like to be able to store each aggregate type in its own table to reduce large
 *       tables in relational event stores. Which constraints would we need do define to allow this?
 */
interface EventStore {
    @PotentialIO
    fun <A : Aggregate> loadEvents(aggregate: Class<A>, aggregateId: String): List<StoredEventData<Event>>

    @PotentialIO
    fun <A : Aggregate> loadEvents(aggregate: Class<A>, minSerial: Long): List<StoredEventData<Event>>

    @PotentialIO
    fun loadEvents(minSerial: Long): List<StoredEventData<Event>>

    @PotentialIO
    fun <E : Event> saveEvent(eventData: ProducedEventData<E>)
}
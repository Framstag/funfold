package com.framstag.funfold.eventstore

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Event

/**
 * The event store allows storing and loading of events.
 *
 * The event store promises the following guaranties:
 * * Events for an aggregate instance are stored with an increasing serial id
 * * Events for an aggregate instance are stored with a unique version in relation to the
 *   instance (the combination of aggregate name, id and version must be unique)
 * * Some implementations also may enforce that versions are monotonically increasing
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
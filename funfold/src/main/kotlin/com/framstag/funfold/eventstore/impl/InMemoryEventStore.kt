package com.framstag.funfold.eventstore.impl

import com.framstag.funfold.cqrs.*
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.eventstore.ProducedEventData
import com.framstag.funfold.eventstore.StoredEventData
import com.framstag.funfold.exception.ConcurrencyViolationException
import mu.KLogging
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class InMemoryEventStore : EventStore {
    companion object : KLogging()

    private val lock = ReentrantReadWriteLock()
    private val eventList: MutableList<StoredEventData<Event>> = mutableListOf()
    private val currentVersionMap: MutableMap<String, Long> = mutableMapOf()

    private fun getAggregateIdentifier(aggregateName: String, aggregateId: String):String {
        return "$aggregateName~$aggregateId"
    }

    private fun getCurrentVersion(aggregateName: String, aggregateId: String): Long? {
        return currentVersionMap[getAggregateIdentifier(aggregateName,aggregateId)]
    }

    private fun setCurrentVersion(aggregateName: String, aggregateId: String, version: Long) {
        return currentVersionMap.set(getAggregateIdentifier(aggregateName,aggregateId), version)
    }

    override fun <A : Aggregate> loadEvents(aggregate: Class<A>, aggregateId: String): List<StoredEventData<Event>> {
        lock.readLock().withLock {
            val aggregateName = aggregate.name
            return eventList.filter {
                it.aggregateName == aggregateName && it.aggregateId == aggregateId
            }
        }
    }

    override fun <A : Aggregate> loadEvents(aggregate: Class<A>, minSerial: Long): List<StoredEventData<Event>> {
        lock.readLock().withLock {
            val aggregateName = aggregate.name

            return eventList.filter {
                it.aggregateName == aggregateName && it.serial >= minSerial
            }
        }
    }

    override fun loadEvents(minSerial: Long): List<StoredEventData<Event>> {
        lock.readLock().withLock {
            return eventList.filter {
                it.serial >= minSerial
            }
        }
    }

    override fun <E : Event> saveEvent(eventData: ProducedEventData<E>) {
        logger.info("Saving event ${eventData.aggregateName} ${eventData.aggregateId} ${eventData.version}")

        lock.writeLock().withLock {
            val currentVersionInStore = getCurrentVersion(eventData.aggregateName,eventData.aggregateId)

            if (currentVersionInStore != null) {
                if (currentVersionInStore+1!=eventData.version) {
                    throw ConcurrencyViolationException("Last version in store is $currentVersionInStore, event version ${eventData.version}")
                }
            }
            else {
                if (eventData.version != 0L) {
                    throw ConcurrencyViolationException("No event in store, event version ${eventData.version} (and thus != 0)")
                }
            }

            val storedEventData = StoredEventData(
                eventList.size.toLong(),
                eventData.aggregateName,
                eventData.aggregateId,
                eventData.aggregateId.hashCode(),
                eventData.version,
                eventData.event
            )

            @Suppress("UNCHECKED_CAST")
            eventList.add(storedEventData as StoredEventData<Event>)

            setCurrentVersion(eventData.aggregateName, eventData.aggregateId,eventData.version)
        }
    }
}
package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.eventprocessor.EventDispatcher
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.transaction.TransactionManager
import mu.KLogging
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

/**
 * Simple event processor that calls event handler using the given event dispatcher for all
 * events in the given event store. Every event is processed once, state is keeped in memory and is
 * lost.
 */
class InMemoryEventProcessor(
    private val dispatcher: EventDispatcher,
    private val eventStore: EventStore,
    private val transactionManager: TransactionManager
) {
    companion object : KLogging()

    private val lock = ReentrantReadWriteLock()
    private val aggregateSerialMap: MutableMap<Class<out Aggregate>, Long> = mutableMapOf()

    private fun getNextSerial(aggregate: Class<out Aggregate>): Long {
        val lastSerial = aggregateSerialMap[aggregate]

        if (lastSerial != null) {
            return lastSerial + 1
        } else {
            return 1
        }
    }

    private fun commitSerial(aggregate: Class<out Aggregate>, serial: Long) {
        aggregateSerialMap[aggregate] = serial
    }

    fun process() {
        lock.writeLock().withLock {
            val aggregates = dispatcher.getObservedAggregates()

            for (aggregate in aggregates) {
                val serial = getNextSerial(aggregate)

                logger.info("Loadings events for ${aggregate.name} starting with serial $serial...")

                @Suppress("UNCHECKED_CAST")
                val events = eventStore.loadEvents(aggregate, serial)

                for (event in events) {
                    transactionManager.execute {
                        dispatcher.dispatch(aggregate, event.event)

                        commitSerial(aggregate, event.serial)
                    }
                }
            }
        }
    }
}
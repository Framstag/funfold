package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.EventDispatcher
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.transaction.TransactionManager
import mu.KLogging
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

/**
 * Simple event processor that calls event handler using the given event dispatcher for all
 * events in the given event store. Every event is processed once, state is kept in memory and is
 * lost.
 */
class InMemoryEventProcessor(
    private val dispatcher: EventDispatcher,
    private val eventStore: EventStore,
    private val transactionManager: TransactionManager
) {
    companion object : KLogging()

    private val lock = ReentrantReadWriteLock()
    private var lastSerial: Long? = null

    private fun getNextSerial(): Long {
        if (lastSerial != null) {
            return lastSerial!! + 1
        } else {
            return 1
        }
    }

    private fun commitSerial(serial: Long) {
        lastSerial = serial
    }

    fun process() {
        lock.writeLock().withLock {
            val serial = getNextSerial()

            logger.info("Loadings events starting with serial $serial...")

            @Suppress("UNCHECKED_CAST")
            val events = eventStore.loadEvents(serial)

            for (event in events) {
                transactionManager.execute {
                    dispatcher.dispatch(event.event)

                    commitSerial(event.serial)
                }
            }
        }
    }
}
package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.cqrs.Event
import com.framstag.funfold.eventprocessor.BucketDistributor
import com.framstag.funfold.eventprocessor.BucketStateStore
import com.framstag.funfold.eventprocessor.EventDispatcher
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.eventstore.StoredEventData
import com.framstag.funfold.transaction.TransactionManager
import mu.KotlinLogging

/**
 * JDBC based event processor
 *
 * Work in progress.
 */
class JDBCEventProcessor(
    private val transactionManager: TransactionManager,
    private val dispatcher: EventDispatcher,
    private val eventStore: EventStore,
    private val bucketDistributor : BucketDistributor,
    private val bucketStateStore: BucketStateStore) {

    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private fun loadPotentialNewEvents():List<StoredEventData<Event>> {
        var storedEvents : List<StoredEventData<Event>> = listOf()
        transactionManager.execute {
            val myBuckets = bucketDistributor.getBuckets()
            val bucketLastSerial: MutableMap<Int,Long?> = mutableMapOf()

            myBuckets.forEach {bucket->
                val lastSerial = bucketStateStore.getBucketState(bucket)

                bucketLastSerial[bucket] = lastSerial
            }

            var minSerial: Long? = null
            if (!bucketLastSerial.values.isEmpty()) {
                minSerial = bucketLastSerial.values.reduce { acc, l ->
                    if (acc != null && l != null) {
                        Math.min(acc,l)
                    } else if (acc!=null) {
                        acc
                    } else {
                        l
                    }}
            }

            val serial = if (minSerial == null) 0 else minSerial +1

            logger.info("Loading events starting with serial $serial")

            storedEvents = eventStore.loadEvents(serial)

            logger.info("Found ${storedEvents.size} events")
        }

        return storedEvents
    }

    fun process() {
        val storedEvents = loadPotentialNewEvents()

        storedEvents.forEach {

            transactionManager.execute {
                val bucket = bucketDistributor.getBucket(it.hash)
                logger.info("Processing event ${it.serial} ${it.hash} in bucket $bucket")
                if (bucketDistributor.shouldProcess(bucket)) {
                    logger.info("Event is part of local bucket")
                    val lastSerial = bucketStateStore.getBucketState(bucket)

                    if (lastSerial == null || it.serial>lastSerial) {
                        logger.info("Dispatching event ${it.event}")
                        dispatcher.dispatch(it.event)

                        logger.info("Updating bucket $bucket to serial ${it.serial}")
                        bucketStateStore.storeBucketState(bucket,lastSerial,it.serial)
                    }
                }
            }
        }
    }
}
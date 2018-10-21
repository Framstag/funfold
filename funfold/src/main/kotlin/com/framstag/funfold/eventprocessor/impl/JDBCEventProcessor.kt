package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.BucketDistributor
import com.framstag.funfold.eventprocessor.BucketStateStore
import com.framstag.funfold.eventprocessor.EventDispatcher
import com.framstag.funfold.eventstore.EventStore
import mu.KotlinLogging

/**
 * JDBC based event processor
 *
 * Work in progress.
 */
class JDBCEventProcessor(
    private val dispatcher: EventDispatcher,
    private val eventStore: EventStore,
    private val bucketDistributor : BucketDistributor,
    private val bucketStateStore: BucketStateStore) {

    companion object {
        private val logger = KotlinLogging.logger {}
    }

    fun process() {
        val myBuckets = bucketDistributor.getBuckets()
        val bucketLastSerial: MutableMap<Int,Long?> = mutableMapOf()

        myBuckets.forEach {bucket->
            val lastSerial = bucketStateStore.getBucketState(bucket)

            bucketLastSerial[bucket] = lastSerial
        }

        var minSerial: Long? = null
        if (bucketLastSerial.values.isEmpty()) {
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

        val storedEvents = eventStore.loadEvents(serial)

        storedEvents.forEach {
            //dispatcher.dispatch(it.)
        }
    }
}
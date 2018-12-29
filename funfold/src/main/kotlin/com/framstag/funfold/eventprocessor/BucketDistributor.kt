package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO

/**
 * We assume that a event processor will be instantiated over multiple processes (and threads) to work on
 * processing the event stream in parallel to scale its throughput.
 *
 * We assume that the same aggregate should not be processed in multiple processes at the same time (since this will
 * likely create race conditions, event consistency effects and similar bad stuff).
 *
 * We assume thus that processor instances have to get a distinct work load and have to (equally) distribute work
 * among them.
 *
 * We assume that we wil have situations where one or multiple processor instances die or get started again.
 * So work load cannot be statically (by configuration) distributed but that we need some dynamic load balancing where
 * work load needs to get rebalanced if state of processors changes.
 *
 * We target at least once event processing. So transactional behaviour and load distribution does not need to be perfect.
 * It though should assure that in normal conditions (no errors) events are only processed once by one processor
 * instance.
 *
 * A bucket distributor assumes that aggregates (and thus events) can get cleanly distributed over multiple buckets
 * using some hashing (normally of the aggregate id). It is the work of a bukcet distributor instance to
 * to the liveness probing and work load distribution using some central heuristic measurements.
 *
 * A BucketDistributor is instantiated for each event processor type in each process.
 */
interface BucketDistributor {
    /**
     * Returns the hash code for a given event
     */
    fun getBucket(hash: Int):Int

    /**
     * Return the list of buckets to be processed by the current process
     */
    @PotentialIO
    fun getBuckets():List<Int>

    /**
     * Returns true, if a bucket should be processed by the current proccess.
     */
    fun shouldProcess(bucket: Int):Boolean
}
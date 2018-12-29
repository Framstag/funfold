package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO

/**
 * A Bucket State Store stores the state of event processing for a given bucket and for a given
 * event processor. The state is stored by memorizing the last processed id.
 *
 * You have to create a BucketStateStore once for each event processor type in each process.
 */
interface BucketStateStore {
    @PotentialIO
    fun getBucketState(bucket:Int):Long?

    @PotentialIO
    fun storeBucketState(bucket: Int, lastSerial: Long?, serial: Long)
}
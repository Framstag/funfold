package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO

interface BucketStateStore {
    @PotentialIO
    fun getBucketState(bucket:Int):Long?

    @PotentialIO
    fun storeBucketState(bucket: Int, lastSerial: Long?, serial: Long)
}
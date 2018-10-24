package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO

interface BucketDistributor {
    fun getBucket(hash: Int):Int

    @PotentialIO
    fun getBuckets():List<Int>

    fun shouldProcess(bucket: Int):Boolean
}
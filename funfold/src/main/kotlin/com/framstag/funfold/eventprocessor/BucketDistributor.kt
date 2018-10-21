package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO

interface BucketDistributor {
    @PotentialIO
    fun getBuckets():List<Int>
}
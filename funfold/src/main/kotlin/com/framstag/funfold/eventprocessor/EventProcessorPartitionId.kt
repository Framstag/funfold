package com.framstag.funfold.eventprocessor

data class EventProcessorPartitionId(val eventInstanceProcessorId : EventProcessorInstanceId, val partitionId : Int)
package com.framstag.funfold.eventprocessor

interface PartitionPositionStore {
    fun storePosition(partitionId : EventProcessorPartitionId, oldSerial: Long, serial : Long)
    fun getPosition(partitionId : EventProcessorPartitionId):Long
    fun resetPosition(processorId : String)
}
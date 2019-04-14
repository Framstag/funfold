package com.framstag.funfold.eventprocessor

interface EventProcessorStore {
    fun updateProcessor(description: EventProcessorDescription)
    fun getPartitions():List<EventProcessorPartitionId>
    fun deleteProcessor(id : EventProcessorInstanceId)
    fun tick()
}
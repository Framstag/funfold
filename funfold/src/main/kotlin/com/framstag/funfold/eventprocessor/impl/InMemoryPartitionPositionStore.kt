package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.EventProcessorPartitionId
import com.framstag.funfold.eventprocessor.PartitionPositionStore
import com.framstag.funfold.exception.ConcurrencyViolationException
import mu.KLogging

class InMemoryPartitionPositionStore : PartitionPositionStore {
    companion object : KLogging()

    private val partitionMap : MutableMap<EventProcessorPartitionId,Long> = mutableMapOf()

    override fun storePosition(partitionId: EventProcessorPartitionId, oldSerial: Long, serial: Long) {
        synchronized(this) {
            val currentSerial = partitionMap.getOrDefault(partitionId,0L)

            if (currentSerial !=oldSerial) {
                throw ConcurrencyViolationException("Partition $partitionId: Expected current position $oldSerial, but current position is $currentSerial")
            }

            logger.info("Advancing position of partition $partitionId from $oldSerial to $serial")
            partitionMap[partitionId] = serial
        }
    }

    override fun getPosition(partitionId: EventProcessorPartitionId): Long {
        synchronized(this) {
            return partitionMap.getOrDefault(partitionId,0L)
        }
    }

    override fun resetPosition(processorId: String) {
        synchronized(this) {
            val partitions = partitionMap
                .map {
                    it.key
                }
                .filter {
                    it.eventInstanceProcessorId.processorId == processorId
                }

            for (partition in partitions) {
                partitionMap.remove(partition)
            }
        }
    }
}
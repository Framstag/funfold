package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.EventProcessorPartitionId
import com.framstag.funfold.eventprocessor.EventProcessorDescription
import com.framstag.funfold.eventprocessor.EventProcessorInstanceId
import com.framstag.funfold.eventprocessor.EventProcessorStore
import com.framstag.funfold.exception.ConfigurationException
import mu.KLogging

class InMemoryEventProcessorStore : EventProcessorStore {
    companion object : KLogging()

    private data class ProcessorEntry(val id : EventProcessorInstanceId,
                                      val partitionCount : Int)

    private val processorMap : MutableMap<EventProcessorInstanceId, ProcessorEntry> = mutableMapOf()
    private val partitions : MutableSet<EventProcessorPartitionId> = mutableSetOf()

    private fun checkForSamePartitionSize(processorId: String) {
        val bucketList = processorMap
            .map {
                mapEntry -> mapEntry.value
            }
            .filter {
                value -> value.id.processorId==processorId
            }
            .map {
                entry -> entry.partitionCount
            }
            .groupBy {
                it
            }

        if (bucketList.size != 1) {
            throw ConfigurationException("Different bucket counts for event processor '$processorId'")
        }
    }

    private fun calculateProcessorCount(processorId: String):Int {
        return processorMap
            .filter {
                it.value.id.processorId==processorId
            }
            .count()
    }

    private fun getProcessorPartitionCount(processorId: String):Int {
        return processorMap
            .filter {
                it.value.id.processorId == processorId
            }
            .map {
                it.value.partitionCount
            }
            .first()
    }

    private fun deletePartitions(processorInstanceId : EventProcessorInstanceId) {
        val partitions = partitions
            .filter {
                it.eventInstanceProcessorId == processorInstanceId
            }

        partitions.forEach {
            this.partitions.remove(it)
        }
    }

    private fun allocatePartitionShare(processorInstanceId : EventProcessorInstanceId) {
        val processorCount=calculateProcessorCount(processorInstanceId.processorId)
        val partitionCount = getProcessorPartitionCount(processorInstanceId.processorId)

        var share = partitionCount / processorCount

        deletePartitions(processorInstanceId)

        for (partition in 0..partitionCount) {
            if (share == 0) {
                return
            }

            val partitionId = EventProcessorPartitionId(processorInstanceId,partition)

            if (partitions.find {
                    it.eventInstanceProcessorId.processorId == processorInstanceId.processorId &&
                            it.partitionId == partitionId.partitionId}==null) {
                partitions.add(partitionId)
                share--
            }
        }
    }

    private fun recalculatePartitions() {
        partitions.clear()

        for (instanceId in processorMap.keys) {
            allocatePartitionShare(instanceId)
        }
    }

    override fun updateProcessor(description: EventProcessorDescription) {
        synchronized(this) {
            logger.info("Updating $description")

            processorMap[description.id]=ProcessorEntry(description.id,description.partitions)

            checkForSamePartitionSize(description.id.processorId)
            recalculatePartitions()
        }
    }

    override fun getPartitions(): List<EventProcessorPartitionId> {
        synchronized(this) {
            return partitions.toList()
        }
    }

    override fun deleteProcessor(id: EventProcessorInstanceId) {
        synchronized(this) {
            logger.info("Deleting $id")

            processorMap.remove(id)
            recalculatePartitions()
        }
    }

    override fun tick() {
        synchronized(this) {
            recalculatePartitions()
        }
    }
}
package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.*
import com.framstag.funfold.eventstore.EventStore
import mu.KLogging
import kotlin.concurrent.thread

class SimpleEventProcessingService(private val eventStore : EventStore,
                                   private val registry : EventProcessorRegistry,
                                   private val partitionStore : PartitionPositionStore) : EventProcessingService {
    companion object : KLogging()

    private data class Entry(val description: EventProcessorDescription,
                             val dispatcher : EventDispatcher)

    private val registryThread = thread(start = false, isDaemon = false, block = ::process)
    private val processorMap : MutableMap<EventProcessorInstanceId,Entry> = mutableMapOf()

    private fun hash(id : String, partitionCount: Int):Int {
        val bigHash = id.hashCode().toLong()-Int.MIN_VALUE
        return bigHash.rem(partitionCount).toInt()
    }

    private fun process() {
        while (!Thread.currentThread().isInterrupted) {
            try {
                for (partition in registry.getPartitions()) {
                    var position = partitionStore.getPosition(partition)
                    val events = eventStore.loadEvents(position, 50)

                    val entry = processorMap[partition.eventInstanceProcessorId]

                    entry?.let {
                        // TODO: Handle exceptions
                        // TODO: Handle retry timeout after exceptions
                        for (event in events) {
                            val partitionId = hash(event.aggregateId, entry.description.partitions)

                            if (partitionId == partition.partitionId) {
                                entry.dispatcher.dispatch(event.event)
                            }

                            partitionStore.storePosition(partition, position, event.serial + 1)
                            position = event.serial + 1
                        }
                    }
                }

                Thread.sleep(500)
            }
            catch (e : InterruptedException) {
                return
            }
        }
    }

    override fun start() {
        EventProcessorRegistry.logger.info("Starting SimpleEventProcessingService thread...")

        registryThread.start()

        EventProcessorRegistry.logger.info("Starting SimpleEventProcessingService thread done.")
    }

    override fun stop() {
        EventProcessorRegistry.logger.info("Stopping SimpleEventProcessingService thread...")

        registryThread.interrupt()
        registryThread.join()

        EventProcessorRegistry.logger.info("Stopping SimpleEventProcessingService thread done.")
    }

    override fun registerEventProcessorInstance(
        description: EventProcessorDescription,
        dispatcher: EventDispatcher) {
        processorMap[description.id] = Entry(description,dispatcher)
    }
}
package com.framstag.funfold.eventprocessor

import mu.KLogging
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.concurrent.thread

class EventProcessorRegistry(val store : EventProcessorStore) {

    companion object : KLogging()

    private open class Command
    private class RegisterProcessorCommand(val description : EventProcessorDescription) : Command()
    private class DeregisterProcessorCommand(val instanceId : EventProcessorInstanceId) : Command()

    private val queue = ConcurrentLinkedQueue<Command>()

    private val registryThread = thread(start = false, isDaemon = false, block = ::process)

    private fun process() {
        logger.info("EventProcessorRegistry thread started")
        while (!Thread.currentThread().isInterrupted) {
            try {
                while (!queue.isEmpty()) {
                    when (val command = queue.poll()) {
                        is RegisterProcessorCommand -> {
                            logger.info("Processing $command")
                            store.updateProcessor(command.description)
                        }
                        is DeregisterProcessorCommand -> {
                            logger.info("Processing $command")
                            store.deleteProcessor(command.instanceId)
                        }
                    }
                }

                Thread.sleep(500)

                store.tick()
            }
            catch (e: InterruptedException) {
                logger.info("EventProcessorRegistry thread stopped")
                return
            }
        }

        logger.info("EventProcessorRegistry thread stopped")
    }

    fun start() {
        logger.info("Starting EventProcessorRegistry thread...")

        registryThread.start()

        logger.info("Starting EventProcessorRegistry thread done.")
    }

    fun stop() {
        logger.info("Stopping EventProcessorRegistry thread...")

        registryThread.interrupt()
        registryThread.join()

        logger.info("Stopping EventProcessorRegistry thread done.")
    }

    fun registerProcessor(description : EventProcessorDescription) {
        logger.info("Registering event processor $description...")
        queue.add(RegisterProcessorCommand(description))
    }

    fun deregisterProcessor(instanceId: EventProcessorInstanceId) {
        logger.info("Deregistering event processor $instanceId...")
        queue.add(DeregisterProcessorCommand(instanceId))
    }

    fun getPartitions():List<EventProcessorPartitionId> {
        return store.getPartitions()
    }
}
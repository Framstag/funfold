package com.framstag.funfold.eventprocessor

interface EventProcessingService {

    fun start()
    fun stop()

    fun registerEventProcessorInstance(description : EventProcessorDescription,
                                       dispatcher : EventDispatcher)
}
package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Event
import com.framstag.funfold.exception.ReconfigurationException
import mu.KLogging

/**
 * The event dispatcher has an internal registry of event handler for events. Based on the given configuration
 * it dispatches events to the corresponding event handler - if it exists.
 *
 * The following requirements exist:
 * - The event dispatcher does not assume that events belong to aggregates.
 * - The event dispatcher assumes that in its context each event has at most one (0..1) event handler assigned.
 *
 * CommandDispatcher and EventDispatcher have similar purpose, structure and implementation.
 */
class EventDispatcher {
    companion object : KLogging() {
        var handlerMap: MutableMap<String, EventHandler<Event>> = mutableMapOf()
    }

    private fun getHandler(eventName : String) : EventHandler<Event>? {
        @Suppress("UNCHECKED_CAST")
        return handlerMap[eventName]
    }

    fun <E : Event> registerEventHandler(
        event: Class<E>,
        eventHandler: EventHandler<E>
    ) {
        val existingHandler = handlerMap[event.name]

        if (existingHandler != null) {
            throw ReconfigurationException("Event ${event.name} already has a handler")
        }

        @Suppress("UNCHECKED_CAST")
        handlerMap[event.name] = eventHandler as EventHandler<Event>
    }

    @PotentialIO
    fun <E: Event>dispatch(event : E) {
        val eventName = event::class.java.name
        val handler = getHandler(eventName)

        if (handler != null) {
            logger.info("Processing event ${eventName}...")

            handler.invoke(event)
        }
    }
}
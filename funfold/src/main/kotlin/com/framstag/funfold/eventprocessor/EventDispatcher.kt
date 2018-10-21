package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Event
import mu.KLogging

/**
 * The event dispatcher has an internal registry of event handler for events. Based on the given configuration
 * is dispatches events to the corresponding event handler - if it exists.
 *
 * CommandDispatcher and EventDispatcher have similar purpose.
 *
 * TODO: Commands are global to all aggregates, events though are local to a given aggregate. Which solution makes
 * more sense?
 */
class EventDispatcher {
    private class AggregateData {
        var handlerMap: MutableMap<Class<Event>, EventHandler<Event>> = mutableMapOf()
    }

    companion object : KLogging()

    private val aggregateMap: MutableMap<Class<out Aggregate>, AggregateData> = HashMap()

    private fun <A: Aggregate, E: Event> getHandler(aggregate : Class<out A>, event : Class<E>) : EventHandler<Event>? {
        val aggregateData = aggregateMap[aggregate]

        if (aggregateData == null) {
            return null
        }

        @Suppress("UNCHECKED_CAST")
        return aggregateData.handlerMap[event as Class<Event>]
    }

    fun <A : Aggregate, E : Event> registerEventHandler(
        aggregate: Class<A>,
        event: Class<E>,
        eventHandler: EventHandler<E>
    ) {
        @Suppress("UNCHECKED_CAST")
        val aggregateData = aggregateMap.getOrPut(aggregate as Class<Aggregate>, ::AggregateData)

        @Suppress("UNCHECKED_CAST")
        aggregateData.handlerMap[event as Class<Event>] = eventHandler as EventHandler<Event>
    }

    fun getObservedAggregates() : List<Class<out Aggregate>> {
        return aggregateMap.keys.toList()
    }

    @PotentialIO
    fun <A: Aggregate, E: Event>dispatch(aggregateClass : Class<A>, event : E) {
        val handler = getHandler(aggregateClass, event::class.java)

        if (handler != null) {
            logger.info("Processing event ${event::class.java}...")

            handler.invoke(event)
        }
    }
}
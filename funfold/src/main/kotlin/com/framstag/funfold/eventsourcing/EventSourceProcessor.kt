package com.framstag.funfold.eventsourcing

import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Command
import com.framstag.funfold.cqrs.Event
import com.framstag.funfold.commandbus.*
import com.framstag.funfold.cqrs.CommandResponse
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.eventstore.ProducedEventData
import com.framstag.funfold.eventstore.StoredEventData
import java.util.concurrent.CompletableFuture

/**
 * The event source processor allows you to register aggregate factories and aggregate handlers.
 *
 * It also wraps your event sourced command handler with a special command handler that implements:
 * * loading past events from the event store
 * * building up the aggregate using given aggregate handlers
 * * calls your event sourced command handler with the resulting aggregate
 * * stores the resulting event
 *
 * TODO: Currently only one resulting event is allowed
 */
class EventSourceProcessor(private val eventStore: EventStore) {
    class AggregateData {
        var aggregateFactory: AggregateFactory<Aggregate>? = null
        var handlerMap: MutableMap<String, AggregateHandler<Aggregate, Event>> = mutableMapOf()
    }

    private val aggregateMap: MutableMap<String, AggregateData> = HashMap()

    private fun getCurrentVersion(events: List<StoredEventData<Event>>): Long {
        return if (events.isEmpty()) {
            0
        } else {
            events.last().version
        }
    }

    private fun getAggregateData(aggregateName: String): AggregateData {
        val aggregateData = aggregateMap[aggregateName]

        if (aggregateData == null) {
            throw Exception("Aggregate $aggregateName is not a registered aggregate")
        }

        return aggregateData
    }

    private fun <A : Aggregate, C : Command, CR : CommandResponse> commandHandler(
        handler: EventSourcedCommandHandler<A, C, CR>,
        aggregateClass: Class<A>,
        aggregateData: AggregateData,
        command: C
    ): CompletableFuture<CR> {
        val processingData = handler.getAggregateInfo(command)
        val aggregateId = processingData.aggregateId
        val events = eventStore.loadEvents(aggregateClass, aggregateId)
        val currentVersion = getCurrentVersion(events)
        val newVersion = currentVersion + 1

        var aggregateInstance = aggregateData.aggregateFactory!!.invoke()

        aggregateInstance = events.fold(aggregateInstance) { aggregate, storedEvent ->
            val eventName = storedEvent.event::class.java.name
            val aggregateHandler = aggregateData.handlerMap[eventName]

            if (aggregateHandler == null) {
                throw Exception("No handler for event $eventName registered")
            }

            aggregateHandler(aggregate, storedEvent.event)
        }

        val commandContext = CommandContext(aggregateId, newVersion)

        @Suppress("UNCHECKED_CAST")
        val commandResponse = handler.execute(aggregateInstance as A, command, commandContext)

        if (commandContext.event == null) {
            throw Exception("Command ${command::class.java.name} did not generate an event")
        }

        val event = commandContext.event!!
        val eventData = ProducedEventData(
            aggregateClass.name,
            aggregateId,
            newVersion,
            event
        )

        eventStore.saveEvent(eventData)

        return commandResponse
    }

    fun <A : Aggregate> registerAggregateFactory(
        aggregate: Class<A>,
        aggregateFactory: AggregateFactory<A>
    ) {
        val aggregateName = aggregate.name

        val aggregateData = aggregateMap.getOrPut(aggregateName, ::AggregateData)

        @Suppress("UNCHECKED_CAST")
        aggregateData.aggregateFactory = aggregateFactory
    }

    fun <A : Aggregate, E : Event> registerAggregateHandler(
        aggregate: Class<A>,
        event: Class<E>,
        aggregateHandler: AggregateHandler<A, E>
    ) {
        val aggregateName = aggregate.name
        val eventName = event.name

        val aggregateData = aggregateMap.getOrPut(aggregateName, ::AggregateData)

        @Suppress("UNCHECKED_CAST")
        aggregateData.handlerMap[eventName] = aggregateHandler as AggregateHandler<Aggregate, Event>
    }

    fun <A : Aggregate, C : Command, CR : CommandResponse> wrap(
        aggregateClass: Class<A>,
        handler: EventSourcedCommandHandler<A, C, CR>
    ): CommandHandler<C, CR> {
        val aggregateName = aggregateClass.name
        val aggregateData = getAggregateData(aggregateName)

        if (aggregateData.aggregateFactory == null) {
            throw Exception("No factory for aggregate $aggregateName registered")
        }

        return fun(command: C): CompletableFuture<CR> {
            return commandHandler(handler, aggregateClass, aggregateData, command)
        }
    }
}
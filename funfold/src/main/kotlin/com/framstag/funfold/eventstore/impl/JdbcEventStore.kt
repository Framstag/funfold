package com.framstag.funfold.eventstore.impl

import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Event
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.eventstore.ProducedEventData
import com.framstag.funfold.eventstore.StoredEventData
import com.framstag.funfold.jdbc.ConnectionProvider
import com.framstag.funfold.serialisation.Serializer
import mu.KotlinLogging

class JdbcEventStore(
    val connectionProvider: ConnectionProvider,
    private val serializer: Serializer
) : EventStore {
    companion object {
        private val logger = KotlinLogging.logger {}

        const val INSERT_EVENT_STATEMENT =
            "INSERT INTO Events(aggregateName, aggregateId, hash, version, eventClass, event) VALUES (?,?,?,?,?,?)"
        const val SELECT_BY_AGGREGATE_AND_AGGREGATE_ID =
            "SELECT e.serial, e.aggregateName, e.hash, e.version, e.eventClass, e.event FROM Events e WHERE e.aggregateName = ? AND e.aggregateId = ?"
        const val SELECT_BY_AGGREGATE_AND_MIN_SERIAL =
            "SELECT e.serial, e.aggregateName, e.aggregateId, e.hash, e.version, e.eventClass, event FROM Events e WHERE e.aggregateName = ? AND e.serial>=?"
        const val SELECT_BY_MIN_SERIAL =
            "SELECT e.serial, e.aggregateName, e.aggregateId, e.hash, e.version, e.eventClass, event FROM Events e WHERE e.serial>=?"
    }

    private fun <A : Aggregate> loadEventsInternal(aggregate: Class<A>, aggregateId: String): List<StoredEventData<Event>> {
        val connection = connectionProvider.getConnection()
        val resultList = mutableListOf<StoredEventData<Event>>()

        val statement = connection.prepareStatement(SELECT_BY_AGGREGATE_AND_AGGREGATE_ID)

        statement.use {
            statement.setString(1, aggregate.name)
            statement.setString(2, aggregateId)

            val result = statement.executeQuery()

            while (result.next()) {
                val serial = result.getLong(1)
                val aggregateName = result.getString(2)
                val hash = result.getInt(3)
                val version = result.getLong(4)
                val eventClass = result.getString(5)
                val eventString = result.getString(6)

                logger.info("Event json: $eventString")

                val storedEvent = StoredEventData(
                    serial,
                    aggregateName,
                    aggregateId,
                    hash,
                    version,
                    serializer.fromString(eventString, eventClass)
                )

                resultList.add(storedEvent)
            }

            connection.commit()

            return resultList
        }
    }

    private fun <A: Aggregate> loadEventsInternal(aggregate: Class<A>, minSerial: Long): List<StoredEventData<Event>> {
        val connection = connectionProvider.getConnection()
        val resultList = mutableListOf<StoredEventData<Event>>()

        val statement = connection.prepareStatement(SELECT_BY_AGGREGATE_AND_MIN_SERIAL)

        statement.use {
            statement.setString(1, aggregate.name)
            statement.setLong(2, minSerial)

            val result = statement.executeQuery()

            while (result.next()) {
                val serial = result.getLong(1)
                val aggregateName = result.getString(2)
                val aggregateId = result.getString(3)
                val hash = result.getInt(4)
                val version = result.getLong(5)
                val eventClass = result.getString(6)
                val eventString = result.getString(7)

                val storedEvent = StoredEventData(
                    serial,
                    aggregateName,
                    aggregateId,
                    hash,
                    version,
                    serializer.fromString(eventString, eventClass)
                )

                resultList.add(storedEvent)
            }

            connection.commit()

            return resultList
        }
    }

    private fun loadEventsInternal(minSerial: Long): List<StoredEventData<Event>> {
        val connection = connectionProvider.getConnection()
        val resultList = mutableListOf<StoredEventData<Event>>()

        val statement = connection.prepareStatement(SELECT_BY_MIN_SERIAL)

        statement.use {
            statement.setLong(1, minSerial)

            val result = statement.executeQuery()

            while (result.next()) {
                val serial = result.getLong(1)
                val aggregateName = result.getString(2)
                val aggregateId = result.getString(3)
                val hash = result.getInt(4)
                val version = result.getLong(5)
                val eventClass = result.getString(6)
                val eventString = result.getString(7)

                val storedEvent = StoredEventData(
                    serial,
                    aggregateName,
                    aggregateId,
                    hash,
                    version,
                    serializer.fromString(eventString, eventClass)
                )

                resultList.add(storedEvent)
            }

            connection.commit()

            return resultList
        }
    }

    override fun <A : Aggregate> loadEvents(aggregate: Class<A>, aggregateId: String): List<StoredEventData<Event>> {
        logger.info("Load all events for aggregate ${aggregate.name} with id $aggregateId")

        return loadEventsInternal(aggregate, aggregateId)
    }

    override fun <A : Aggregate> loadEvents(aggregate: Class<A>, minSerial: Long): List<StoredEventData<Event>> {
        logger.info("Load all events for aggregate ${aggregate.name} starting from $minSerial")

        return loadEventsInternal(aggregate, minSerial)
    }

    override fun loadEvents(minSerial: Long): List<StoredEventData<Event>> {
        logger.info("Load all events starting from $minSerial")

        return loadEventsInternal(minSerial)
    }

    private fun <E: Event>saveEventInternal(serializer : Serializer, eventData: ProducedEventData<E>) {
        val connection = connectionProvider.getConnection()

        val statement = connection.prepareStatement(INSERT_EVENT_STATEMENT)
        val eventString=serializer.toString(eventData.event)

        statement.use {
            statement.setString(1, eventData.aggregateName)
            statement.setString(2, eventData.aggregateId)
            statement.setInt(3, eventData.aggregateId.hashCode())
            statement.setLong(4, eventData.version)
            statement.setString(5, eventData.event::class.java.name)
            statement.setString(6, eventString)

            statement.executeUpdate()
        }
    }

    override fun <E : Event> saveEvent(eventData: ProducedEventData<E>) {
        logger.info("Saving event ${eventData.aggregateName} ${eventData.aggregateId} ${eventData.version}")

        saveEventInternal(serializer, eventData)
    }
}
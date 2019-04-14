package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.demo.addressbook.processor.onPersonCreatedEventHandler
import com.framstag.funfold.demo.addressbook.processor.onPersonMarriedEventHandler
import com.framstag.funfold.commandbus.CommandDispatcher
import com.framstag.funfold.commandbus.impl.SimpleCommandBus
import com.framstag.funfold.eventprocessor.EventDispatcher
import com.framstag.funfold.eventprocessor.impl.*
import com.framstag.funfold.eventsourcing.EventSourceProcessor
import com.framstag.funfold.eventstore.EventStore
import com.framstag.funfold.eventstore.impl.JDBCEventStoreH2SchemaGenerator
import com.framstag.funfold.eventstore.impl.InMemoryEventStore
import com.framstag.funfold.eventstore.impl.JdbcEventStore
import com.framstag.funfold.jdbc.impl.JavaSEConnectionProvider
import com.framstag.funfold.transaction.JavaSETransactionManager
import com.framstag.funfold.transaction.TransactionManager
import com.framstag.funfold.demo.addressbook.serialisation.JsonSerializer
import com.framstag.funfold.eventprocessor.EventProcessorDescription
import com.framstag.funfold.eventprocessor.EventProcessorInstanceId
import com.framstag.funfold.eventprocessor.EventProcessorRegistry
import com.framstag.funfold.serialisation.Serializer
import mu.KotlinLogging
import org.h2.jdbcx.JdbcDataSource
import java.util.*

val logger = KotlinLogging.logger("main")

fun getInMemoryEventStore(): EventStore {
    return InMemoryEventStore()
}

fun getJDBCEventStore(transactionManager: TransactionManager, serializer: Serializer): EventStore {
    val datasource = JdbcDataSource()
    val connectionProvider = JavaSEConnectionProvider(datasource)

    datasource.setUrl("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1")
    datasource.user = "sa"
    datasource.password = "sa"

    val storeSchemaGenerator = JDBCEventStoreH2SchemaGenerator(connectionProvider, transactionManager)

    logger.info("Creating event store schema...")
    storeSchemaGenerator.generate()
    logger.info("Creating event store schema...done")

    val processorSchemaGenerator = JDBCEventProcessorH2SchemaGenerator(connectionProvider, transactionManager)

    logger.info("Creating event processor schema...")
    processorSchemaGenerator.generate()
    logger.info("Creating event processor schema...done")

    return JdbcEventStore(connectionProvider, serializer)
}

fun main() {
    // Transaction handling
    val transactionManager = JavaSETransactionManager()

    val serializer = JsonSerializer()

    // Event store
    val eventStore = getInMemoryEventStore()
    //val eventStore = getJDBCEventStore(transactionManager,serializer)

    // EventSourcing
    val eventSourcedProcessor = EventSourceProcessor(eventStore)

    // Aggregate Person

    eventSourcedProcessor.registerAggregateFactory(
        Person::class.java,
        ::Person
    )
    eventSourcedProcessor.registerAggregateHandler(
        Person::class.java,
        PersonCreatedEvent::class.java,
        ::personCreatedHandler
    )

    eventSourcedProcessor.registerAggregateHandler(
        Person::class.java,
        PersonMarriedEvent::class.java,
        ::personMarriedHandler
    )

    // Command bus
    val commandDispatcher = CommandDispatcher()

    // Commands

    commandDispatcher.registerCommandHandler(
        CreatePersonCommand::class.java,
        eventSourcedProcessor.wrap(
            Person::class.java,
            CreatePersonCommandHandler()
        )
    )

    commandDispatcher.registerCommandHandler(
        PersonMarriageCommand::class.java,
        eventSourcedProcessor.wrap(
            Person::class.java,
            PersonMarriageCommandHandler()
        )
    )

    val commandBus = SimpleCommandBus(commandDispatcher)

    logger.info("Start sending commands...")

    val personId = UUID.randomUUID().toString()
    var lastVersion: Long = -1

    transactionManager.execute {
        val createPersonCommand =
            CreatePersonCommand(personId, "Tim", "Teulings")

        val response: CreatePersonCommandResponse = commandBus.executeSync(createPersonCommand)

        lastVersion = response.version
    }

    transactionManager.execute {
        val personMarriageCommand =
            PersonMarriageCommand(personId, lastVersion, "Basso")

        val response: PersonMarriageCommandResponse = commandBus.executeSync(personMarriageCommand)

        lastVersion = response.version
    }

    logger.info("Start sending commands...done")

    transactionManager.execute {
        logger.info("List of events in event store for Person $personId: ")

        val events = eventStore.loadEvents(Person::class.java, personId)

        events.forEach {
            logger.info("* $it")
        }
    }

    // EventDispatcher

    val eventDispatcher = EventDispatcher()

    eventDispatcher.registerEventHandler(
        PersonCreatedEvent::class.java,
        ::onPersonCreatedEventHandler
    )
    eventDispatcher.registerEventHandler(
        PersonMarriedEvent::class.java,
        ::onPersonMarriedEventHandler
    )

    /* ----- */

    val eventProcessorStore = InMemoryEventProcessorStore()
    val eventProcessorRegistry = EventProcessorRegistry(eventProcessorStore)
    val partitionStore = InMemoryPartitionPositionStore()

    val processorName = "Test"
    val processorPartitions = 20
    val processor1Description = EventProcessorDescription(EventProcessorInstanceId(processorName,"1"),processorPartitions)
    val processor2Description = EventProcessorDescription(EventProcessorInstanceId(processorName,"2"),processorPartitions)

    eventProcessorRegistry.start()
    eventProcessorRegistry.registerProcessor(processor1Description)
    eventProcessorRegistry.registerProcessor(processor2Description)

    // Give the asynchronous registry some time...
    Thread.sleep(1000)

    logger.info("Partitions: ${eventProcessorRegistry.getPartitions()}")

    val eventProcessingService = SimpleEventProcessingService(eventStore,eventProcessorRegistry,partitionStore)

    eventProcessingService.registerEventProcessorInstance(processor1Description,eventDispatcher)
    eventProcessingService.registerEventProcessorInstance(processor2Description,eventDispatcher)

    eventProcessingService.start()

    Thread.sleep(10000)

    eventProcessingService.stop()

    // BucketProcessor

    /*
    if (eventStore is JdbcEventStore) {
       logger.info("Defining jdbc processor...")

        val processorId="Test"
        val bucketCount = 20
        val refreshTime = 120

        val bucketStateStore = JDBCBucketStateStore(
            transactionManager,
            eventStore.connectionProvider,
            processorId
        )

        val bucketDistributor1 = JDBCBucketDistributor(
            transactionManager,
            eventStore.connectionProvider,
            processorId,
            "1",
            bucketCount,
            refreshTime
        )

        val jdbcProcessor1 =
            JDBCEventProcessor(transactionManager,
                eventDispatcher,
                eventStore,
                bucketDistributor1,
                bucketStateStore)

        val bucketDistributor2 = JDBCBucketDistributor(
            transactionManager,
            eventStore.connectionProvider,
            processorId,
            "2",
            bucketCount,
            refreshTime
        )

        val jdbcProcessor2 =
            JDBCEventProcessor(transactionManager,
                eventDispatcher,
                eventStore,
                bucketDistributor2,
                bucketStateStore)

        logger.info("Defining jdbc processor...done")

        logger.info("Starting processor in loop...")

        val thread1 = thread(name="Thread 1", block={
            while (true) {
                transactionManager.execute {
                    jdbcProcessor1.process()
                }
                Thread.sleep(1000)
            }
        })

        val thread2 = thread(name="Thread 2", block={
            while (true) {
                transactionManager.execute {
                    jdbcProcessor2.process()
                }
                Thread.sleep(1000)
            }
        })

        thread1.join()
        thread2.join()

        logger.info("Starting processor in loop...done")
    }
    else {
        logger.info("Defining and starting in-memory processor...")

        val inMemoryProcessor = InMemoryEventProcessor(eventDispatcher, eventStore, transactionManager)

        logger.info("Defining and starting in-memory processor...done")

        logger.info("Starting processor in loop...")

        while (true) {
            transactionManager.execute {
                inMemoryProcessor.process()
            }
            Thread.sleep(1000)
        }

        logger.info("Starting processor in loop...done")
    }*/

    eventProcessorRegistry.stop()
}
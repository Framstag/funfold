package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.jdbc.ConnectionProvider
import com.framstag.funfold.transaction.TransactionManager

class JDBCEventProcessorH2SchemaGenerator(private val connectionProvider: ConnectionProvider,
                                          private val transactionManager: TransactionManager
) {
    companion object {
        const val CREATE_TABLE_PROCESSORS =
            """CREATE TABLE Processors(processorId varchar(255) NOT NULL,
                    instanceId varchar(255) NOT NULL,
                    creationTime timestamp NOT NULL,
                    finishTime timestamp NOT NULL,
                    buckets int NOT NULL,
                    PRIMARY KEY (processorId,instanceId))"""

        const val CREATE_TABLE_PROCESSOR_BUCKETS =
            """CREATE TABLE Processor_Buckets(processorId varchar(255) NOT NULL,
                    instanceId varchar(255) NOT NULL,
                    creationTime timestamp NOT NULL,
                    finishTime timestamp NOT NULL,
                    bucket int NOT NULL,
                    PRIMARY KEY (processorId,instanceId,bucket),
                    FOREIGN KEY (processorId,instanceId) REFERENCES Processors(processorId,instanceId))"""

        const val CREATE_TABLE_PROCESSOR_OFFSETS =
            """CREATE TABLE Processor_Buckets_Offsets(processorId varchar(255) NOT NULL,
                    bucket int NOT NULL,
                    lastSerial bigint NOT NULL,
                    PRIMARY KEY (processorId,bucket))"""

        const val CREATE_UNIQUE_INDEX_PROCESSOR_BUCKET =
            "CREATE UNIQUE INDEX Index_Processor_Buckets ON Processor_Buckets(processorId,bucket)"
    }

    fun generate() {
        transactionManager.execute {
            val connection=connectionProvider.getConnection()

            val statement = connection.createStatement()

            statement.use {
                statement.executeUpdate(CREATE_TABLE_PROCESSORS)
                statement.executeUpdate(CREATE_TABLE_PROCESSOR_BUCKETS)
                statement.executeUpdate(CREATE_TABLE_PROCESSOR_OFFSETS)
                statement.executeUpdate(CREATE_UNIQUE_INDEX_PROCESSOR_BUCKET)
            }
        }
    }
}
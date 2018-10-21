package com.framstag.funfold.eventstore.impl

import com.framstag.funfold.jdbc.ConnectionProvider
import com.framstag.funfold.transaction.TransactionManager

class JDBCEventStoreH2SchemaGenerator(private val connectionProvider: ConnectionProvider,
                                      private val transactionManager: TransactionManager
) {
    companion object {
        const val CREATE_TABLE_EVENTS =
            """CREATE TABLE Events(serial bigint auto_increment primary key,
                    aggregateName varchar(255) NOT NULL,
                    aggregateId varchar(36) NOT NULL,
                    hash int NOT NULL,
                    version bigint NOT NULL,
                    eventClass varchar(255) NOT NULL,
                    event varchar(4096) NOT NULL)"""
    }

    fun generate() {
        transactionManager.execute {
            val connection=connectionProvider.getConnection()

            val statement = connection.createStatement()

            statement.use {
                statement.executeUpdate(CREATE_TABLE_EVENTS)

            }
        }
    }
}
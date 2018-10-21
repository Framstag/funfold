package com.framstag.funfold.jdbc.impl

import com.framstag.funfold.jdbc.ConnectionProvider
import com.framstag.funfold.transaction.JavaSETransactionManager
import mu.KLogging
import java.sql.Connection
import javax.sql.DataSource

class JavaSEConnectionProvider(
    private val datasource: DataSource
) : ConnectionProvider {
    companion object : KLogging()

    override fun getConnection(): Connection {
        val currentTransaction = JavaSETransactionManager.getCurrentTransaction()

        if (currentTransaction == null) {
            throw Exception("Yuo are trying to create a connection without a transaction")
        }

        val currentConnection = currentTransaction.getCurrentConnection()

        if (currentConnection != null) {
            return currentConnection
        }

        val newConnection = datasource.connection

        newConnection.autoCommit = false
        currentTransaction.registerConnection(newConnection)

        return newConnection
    }
}

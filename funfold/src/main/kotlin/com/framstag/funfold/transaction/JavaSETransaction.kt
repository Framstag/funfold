package com.framstag.funfold.transaction

import java.sql.Connection

class JavaSETransaction(private val parentTransaction: JavaSETransaction?) : Transaction {
    private var connection : Connection? = null

    override fun commit() {
        if (parentTransaction == null) {

            try {
                connection?.use { connection ->
                    connection.commit()
                }
            }
            finally {
                JavaSETransactionManager.clearTransaction()
            }
        }
    }

    override fun rollback() {
        if (parentTransaction == null) {

            try {
                connection?.use { connection ->
                    if (!connection.isClosed) {
                        connection.rollback()
                    }
                }
            }
            finally {
                JavaSETransactionManager.clearTransaction()
            }
        }
    }


    fun registerConnection(connection : Connection) {
        if (this.connection != null) {
            throw Exception("You cannot assign a new connection to the current transaction")
        }

        this.connection = connection
    }

    fun getCurrentConnection():Connection? {
        return connection
    }
}
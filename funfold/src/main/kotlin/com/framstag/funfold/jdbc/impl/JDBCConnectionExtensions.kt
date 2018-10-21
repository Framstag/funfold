package com.framstag.funfold.jdbc.impl

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.exception.JDBCConstraintViolationException
import com.framstag.funfold.exception.JDBCException
import mu.KotlinLogging
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException

private val logger = KotlinLogging.logger {}

private fun h2SQLExceptionConverter(e : SQLException):JDBCException {
    return when (e.errorCode) {
        23505 -> JDBCConstraintViolationException(e)
        else -> JDBCException(e)
    }
}

fun defaultSQLExceptionConverter(e : SQLException):JDBCException {
    return JDBCException(e)
}

private fun exceptionConverter(connection: Connection, e : SQLException):JDBCException {
    val driverName = connection.metaData.driverName
    return when (driverName) {
        "H2 JDBC Driver" -> h2SQLExceptionConverter(e)
        else -> {
            logger.warn("No exception converter for database driver '$driverName'")
            defaultSQLExceptionConverter(e)
        }
    }
}

@PotentialIO
fun Connection.executeInsert(sql: String, vararg args: Any):Int {
    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            return statement.executeUpdate()
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

@PotentialIO
fun Connection.executeUpdate(sql: String, vararg args: Any):Int {
    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            return statement.executeUpdate()
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

@PotentialIO
fun Connection.executeDelete(sql: String, vararg args: Any):Int {
    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            return statement.executeUpdate()
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

@PotentialIO
fun Connection.executeSelectCount(sql: String, vararg args: Any):Long {

    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            val result = statement.executeQuery()

            if (result.next()) {
                val resultObject = result.getLong(1)

                if (result.next()) {
                    throw Exception("Single result row expected, but multiple rows found!")
                }

                return resultObject
            }
            else {
                throw Exception("Single result row expected, no row found!")
            }
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

@PotentialIO
fun <T> Connection.executeSelectOptionalOne(sql: String, transformer: (ResultSet) -> T, vararg args: Any):T? {

    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            val result = statement.executeQuery()

            if (result.next()) {
                val resultObject = transformer(result)

                if (result.next()) {
                    throw Exception("Single result row expected, but multiple rows found!")
                }

                return resultObject
            }
            else {
                return null
            }
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

@PotentialIO
fun <T> Connection.executeSelectOne(sql: String, transformer: (ResultSet) -> T, vararg args: Any):T {

    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            val result = statement.executeQuery()

            if (result.next()) {
                val resultObject = transformer(result)

                if (result.next()) {
                    throw Exception("Single result row expected, but multiple rows found!")
                }

                return resultObject
            }
            else {
                throw Exception("Single result row expected, no row found!")
            }
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

@PotentialIO
fun <T> Connection.executeSelect(sql: String, transformer: (ResultSet) -> T, vararg args: Any):List<T> {

    try {
        val statement = this.prepareStatement(sql)

        statement.use {
            for ((index, arg) in args.withIndex()) {
                statement.setObject(index + 1, arg)
            }

            val result = statement.executeQuery()
            val resultList: MutableList<T> = mutableListOf()

            while (result.next()) {
                resultList.add(transformer(result))
            }

            return resultList
        }
    }
    catch (e: SQLException) {
        throw exceptionConverter(this,e)
    }
}

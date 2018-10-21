package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.BucketStateStore
import com.framstag.funfold.exception.FunFoldException
import com.framstag.funfold.jdbc.ConnectionProvider
import com.framstag.funfold.jdbc.impl.executeInsert
import com.framstag.funfold.jdbc.impl.executeSelectOptionalOne
import com.framstag.funfold.jdbc.impl.executeUpdate
import com.framstag.funfold.transaction.TransactionManager
import mu.KotlinLogging
import java.sql.ResultSet

class JDBCBucketStateStore(
    private val transactionManager: TransactionManager,
    private val connectionProvider: ConnectionProvider,
    private val processorId: String
    ) : BucketStateStore {

    companion object {
        private val logger = KotlinLogging.logger {}

        private const val SELECT_BUCKET_LAST_SERIAL = "SELECT lastSerial FROM Processor_Buckets_Offsets WHERE processorId=? and bucket=?"
        private const val INSERT_BUCKET = "INSERT INTO Processor_Buckets_Offsets(processorId,bucket,lastSerial) VALUES (?,?,?)"
        private const val UPDATE_BUCKET = "UPDATE Processor_Buckets_Offsets o SET o.processorId=?,o.bucket=?,o.lastSerial=? WHERE o.processorId=? AND o.bucket=? AND o.lastSerial=?"
    }

    private fun resultToLastSerial(result: ResultSet):Long {
        return result.getLong(1)
    }

    private fun insertBucketState(processorId: String, bucket: Int, serial: Long) {
        val connection = connectionProvider.getConnection()

        logger.info("Set lastSerial to $serial for bucket $bucket of processor $processorId")
        connection.executeInsert(INSERT_BUCKET,processorId,bucket,serial)
    }

    private fun updateBucketState(processorId: String, bucket: Int, lastSerial: Long, serial: Long) {
        val connection = connectionProvider.getConnection()

        logger.info("Update lastSerial from $lastSerial to $serial for bucket $bucket of processor $processorId")
        if (connection.executeUpdate(UPDATE_BUCKET,processorId,bucket,serial,processorId,bucket,lastSerial)==0) {
            throw FunFoldException("State conflict during update of bucket store")
        }
    }

    override fun getBucketState(bucket: Int): Long? {
        var lastSerial : Long? = null

        transactionManager.execute {
            val connection = connectionProvider.getConnection()

            lastSerial = connection.executeSelectOptionalOne(SELECT_BUCKET_LAST_SERIAL, ::resultToLastSerial,processorId,bucket)
        }

        return lastSerial
    }

    override fun storeBucketState(bucket: Int, lastSerial : Long?, serial: Long) {
        transactionManager.execute {
            if (lastSerial == null) {
                insertBucketState(processorId,bucket,serial)
            }
            else {
                updateBucketState(processorId,bucket,lastSerial, serial)
            }
        }
    }
}
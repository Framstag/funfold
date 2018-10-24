package com.framstag.funfold.eventprocessor.impl

import com.framstag.funfold.eventprocessor.BucketDistributor
import com.framstag.funfold.exception.JDBCConstraintViolationException
import com.framstag.funfold.jdbc.ConnectionProvider
import com.framstag.funfold.jdbc.impl.*
import com.framstag.funfold.transaction.TransactionManager
import mu.KotlinLogging
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.util.*

class JDBCBucketDistributor(private val transactionManager: TransactionManager,
                            private val connectionProvider: ConnectionProvider,
                            private val processorId: String,
                            private val instanceId: String,
                            private val bucketCount: Int,
                            private val refreshTime: Int) : BucketDistributor {
    companion object {
        private val logger = KotlinLogging.logger {}

        private const val MAX_TIME_DRIFT = 10*1000L

        private const val SELECT_CURRENT_TIMESTAMP =
            "SELECT CURRENT_TIMESTAMP()"

        private const val DELETE_OLD_BUCKETS =
            "DELETE FROM Processor_Buckets pb WHERE pb.finishTime<CURRENT_TIMESTAMP() AND pb.processorId = ?"

        private const val DELETE_OLD_PROCESSORS =
            "DELETE FROM Processors p WHERE NOT EXISTS (SELECT 1 FROM Processor_Buckets pb WHERE pb.processorId=p.processorId AND pb.instanceId=p.instanceId) AND p.processorId=? AND p.finishTime<CURRENT_TIMESTAMP()"

        private const val SELECT_CURRENT_PROCESSORS =
            "SELECT p.processorId, p.instanceId, p.creationTime, p.finishTime, p.buckets FROM Processors p WHERE p.processorId = ? AND p.creationTime<=CURRENT_TIMESTAMP() AND p.finishTime>CURRENT_TIMESTAMP()"

        private const val SELECT_CURRENT_BUCKETS =
            "SELECT pb.processorId, pb.instanceId, pb.creationTime, pb.finishTime, pb.bucket FROM Processor_Buckets pb WHERE pb.processorId = ? AND pb.creationTime<=CURRENT_TIMESTAMP() AND pb.finishTime>CURRENT_TIMESTAMP()"

        private const val INSERT_PROCESSOR =
            """INSERT INTO Processors(processorId,
                instanceId,
                creationTime,
                finishTime,
                buckets) VALUES (?,?,CURRENT_TIMESTAMP(),DATEADD('SECOND',?,CURRENT_TIMESTAMP()),?)"""

        private const val UPDATE_PROCESSOR =
            """UPDATE Processors p SET creationTime=CURRENT_TIMESTAMP(),finishTime=DATEADD('SECOND',?,CURRENT_TIMESTAMP()) WHERE p.processorId=? AND p.instanceId=?"""

        private const val INSERT_BUCKET =
            """INSERT INTO Processor_Buckets(processorId,
                instanceId,
                creationTime,
                finishTime,
                bucket) VALUES (?,?,?,?,?)"""

        private const val UPDATE_BUCKET =
            """UPDATE Processor_Buckets pb SET creationTime=?,finishTime=? WHERE pb.processorId=? AND pb.instanceId=? AND pb.bucket=?"""

        private const val DELETE_BUCKET =
            """DELETE FROM Processor_Buckets pb WHERE pb.processorId=? AND pb.instanceId=? AND pb.bucket=?"""
    }

    data class BucketProcessor(
        val processorId: String,
        val instanceId: String,
        val creationTime: Timestamp,
        val finishTime: Timestamp,
        val buckets: Int
    )

    data class Bucket(
        val processorId: String,
        val instanceId: String,
        val creationTime: Timestamp,
        val finishTime: Timestamp,
        val bucket: Int
    )

    private var nextFinishTime: Long = System.currentTimeMillis()
    private var timeDrift: Long = 0
    private var myBuckets: List<Int> = listOf()

    private fun resultToTimestamp(result: ResultSet): Timestamp {
        return result.getTimestamp(1)
    }

    private fun resultToProcessor(result: ResultSet): BucketProcessor {
        return BucketProcessor(
            processorId = result.getString(1),
            instanceId = result.getString(2),
            creationTime = result.getTimestamp(3),
            finishTime = result.getTimestamp(4),
            buckets = result.getInt(5)
        )
    }

    private fun resultToBucket(result: ResultSet): Bucket {
        return Bucket(
            processorId = result.getString(1),
            instanceId = result.getString(2),
            creationTime = result.getTimestamp(3),
            finishTime = result.getTimestamp(4),
            bucket = result.getInt(5)
        )
    }

    private fun getTimeDrift():Long {
        val connection = connectionProvider.getConnection()

        val ourTimestamp = Timestamp(System.currentTimeMillis())
        val databaseTimestamp = connection.executeSelectOne(SELECT_CURRENT_TIMESTAMP,::resultToTimestamp)

        logger.info("Our timestamp $ourTimestamp,  database $databaseTimestamp")

        return ourTimestamp.time-databaseTimestamp.time
    }

    private fun assertTimeDrift(timeDrift: Long) {
        if (Math.abs(timeDrift)> MAX_TIME_DRIFT) {
            throw Exception("Time difference between database and local system is >${MAX_TIME_DRIFT/1000} seconds")
        }
        if (Math.abs(timeDrift)> refreshTime/10) {
            throw Exception("Time difference between database and local system is 10% of the refresh time")
        }
    }

    private fun deleteOldProcessorInstances() {
        val connection = connectionProvider.getConnection()

        val deleteCountBuckets = connection.executeDelete(
            DELETE_OLD_BUCKETS, processorId
        )

        logger.info("Deleted $deleteCountBuckets old processor buckets")

        val deleteCountProcessors = connection.executeDelete(
            DELETE_OLD_PROCESSORS,processorId)

        logger.info("Deleted $deleteCountProcessors old processor entries")
    }

    private fun getActiveProcessorInstances(): List<BucketProcessor> {
        val connection = connectionProvider.getConnection()

        val processors = connection.executeSelect(SELECT_CURRENT_PROCESSORS,::resultToProcessor, processorId)

        logger.info("Found ${processors.size} processors:")
        processors.forEach {
            logger.info("* $it")
        }

        return processors
    }

    private fun getActiveBuckets(): List<Bucket> {
        val connection = connectionProvider.getConnection()

        val buckets = connection.executeSelect(SELECT_CURRENT_BUCKETS,::resultToBucket,processorId)

        logger.info("Found ${buckets.size} buckets:")
        buckets.forEach {
            logger.info("* $it")
        }

        return buckets
    }

    private fun insertProcessorInstance(): BucketProcessor {
        val connection = connectionProvider.getConnection()

        val currentTime = Timestamp(System.currentTimeMillis())
        val finishTime = Timestamp(System.currentTimeMillis()+refreshTime*1000)

        val result = connection.executeInsert(
            INSERT_PROCESSOR,processorId,instanceId,refreshTime,bucketCount)

        when (result) {
            1 -> return BucketProcessor(processorId,instanceId,currentTime,finishTime,bucketCount)
            else -> throw Exception("Could not insert processor $processorId $instanceId")
        }
    }

    private fun updateProcessorInstance(currentProcessor: BucketProcessor): BucketProcessor {
        val connection = connectionProvider.getConnection()
        val processor = currentProcessor.copy(finishTime = Timestamp(System.currentTimeMillis()+refreshTime*1000))
        val result = connection.executeUpdate(UPDATE_PROCESSOR,refreshTime,processorId,instanceId)

        when (result) {
            1 -> return processor
            else -> throw Exception("Could not insert processor $processorId $instanceId")
        }
    }

    private fun insertBucket(myProcessor: BucketProcessor, bucketId: Int):Boolean {
        try  {
            val connection = connectionProvider.getConnection()
            val result = connection.executeInsert(
                INSERT_BUCKET,
                processorId,
                instanceId,
                myProcessor.creationTime,
                myProcessor.finishTime,
                bucketId
            )

            return result==1
        }
        catch (e: JDBCConstraintViolationException) {
            return false
        }
        catch (e: SQLException) {
            logger.error("SQL Error ${e.errorCode}",e)
            throw e
        }
    }

    private fun updateBucket(myProcessor: BucketProcessor, bucketId: Int):Boolean {
        try {
            val connection = connectionProvider.getConnection()
            val result = connection.executeUpdate(
                UPDATE_BUCKET,
                myProcessor.creationTime,
                myProcessor.finishTime,
                myProcessor.processorId,
                myProcessor.instanceId,
                bucketId
            )

            return result==1
        } catch (e: SQLException) {
            logger.error("SQL Error ${e.errorCode}", e)
            throw e
        }
    }

    private fun deleteBucket(myProcessor: BucketProcessor, bucketId: Int) {
        val connection = connectionProvider.getConnection()

        connection.executeDelete(
            DELETE_BUCKET, myProcessor.processorId,myProcessor.instanceId,bucketId
        )

        logger.info("Deleted my bucket $bucketId")
    }

    private fun shouldRefresh(currentTime: Long):Boolean {
        return currentTime>nextFinishTime-(refreshTime*1000*20)/100
    }

    private fun updateMyProcessorInDatabase(processors: List<BucketProcessor>): BucketProcessor {
        val myProcessorInstance = Optional.ofNullable(processors.find { processor->
            processor.instanceId == instanceId
        })

        return if (myProcessorInstance.isPresent) {
            updateProcessorInstance(myProcessorInstance.get())
        } else {
            insertProcessorInstance()
        }
    }

    private fun getMyShareOfBuckets(processors : List<BucketProcessor>, bucketCount: Int):Int {
        val otherProcessorInstances = processors.filter { processor->
            processor.instanceId != instanceId
        }.size

        val totalProcessorInstances = otherProcessorInstances +1

        val myBucketShare = bucketCount / totalProcessorInstances

        logger.info("We should hold maximum $myBucketShare of $bucketCount buckets ($totalProcessorInstances processor instances)")

        return myBucketShare
    }

    private fun updateBucketAllocation(myProcessor: BucketProcessor, currentBuckets: List<Bucket>, myBucketShare: Int, bucketCount: Int):List<Int> {
        // 0..bucketCount-1
        val allBucketsId = 0 until bucketCount

        val unallocatedBucketIds = allBucketsId.minus(currentBuckets.map {
            it.bucket
        })

        val ourCurrentBucketIds = currentBuckets.asSequence().filter {
            it.instanceId == instanceId
        }.map {
            it.bucket
        }.toList()

        var processableBuckets: List<Int> = ourCurrentBucketIds.plus(unallocatedBucketIds)

        val myBuckets: MutableSet<Int> = mutableSetOf()
        var ourBucketLimit=myBucketShare
        while (ourBucketLimit>0 && processableBuckets.isNotEmpty()) {
            val bucketId = processableBuckets.first()

            processableBuckets = processableBuckets.drop(1)

            val bucket = currentBuckets.find {
                it.instanceId == instanceId && it.bucket == bucketId
            }

            if (bucket != null) {
                if (updateBucket(myProcessor,bucketId)) {
                    logger.info("Updated bucket $bucketId")
                    myBuckets.add(bucketId)
                    ourBucketLimit--
                }
            }
            else if (insertBucket(myProcessor,bucketId)) {
                logger.info("Inserted bucket $bucketId")
                myBuckets.add(bucketId)
                ourBucketLimit--
            }
        }

        for (bucket in currentBuckets) {
            if (bucket.instanceId == instanceId && !myBuckets.contains(bucket.bucket)) {
                deleteBucket(myProcessor,bucket.bucket)
            }
        }

        return myBuckets.toList()
    }

    private fun refreshBuckets() {
        logger.info("Refreshing...")

        // Own transaction to allow fail
        transactionManager.execute {
            deleteOldProcessorInstances()
        }

        transactionManager.execute {
            timeDrift = getTimeDrift()
            assertTimeDrift(timeDrift)
        }

        lateinit var processors : List<BucketProcessor>
        lateinit var myProcessorInstance : BucketProcessor
        lateinit var buckets : List<Bucket>

        var myBucketShare = 0

        transactionManager.execute {
            processors = getActiveProcessorInstances()
            myProcessorInstance = updateMyProcessorInDatabase(processors)
            buckets = getActiveBuckets()

            nextFinishTime = myProcessorInstance.finishTime.time

            myBucketShare = getMyShareOfBuckets(processors, bucketCount)
        }

        // TODO: Allocate each bucket in its own transaction, this way processors starting
        // at the same time may have interleaved allocation
        transactionManager.execute {
            myBuckets = updateBucketAllocation(myProcessorInstance, buckets, myBucketShare, bucketCount)

            logger.info("Allocated buckets: $myBuckets")
        }

        logger.info("Refreshing...done")
    }

    override fun getBucket(hash: Int): Int {
        // We extend the hash code to long to make sure, that we do not get negative
        // bucket numbers - which we obviously can get for int the way the hash code is build
        val bigHash = hash.toLong()-Int.MIN_VALUE;
        return bigHash.rem(bucketCount).toInt()
    }

    override fun shouldProcess(bucket:Int): Boolean {
        val buckets = getBuckets()

        return buckets.contains(bucket)
    }

    override fun getBuckets(): List<Int> {
        if (shouldRefresh(System.currentTimeMillis())) {
            refreshBuckets()
        }

        return myBuckets
   }
}
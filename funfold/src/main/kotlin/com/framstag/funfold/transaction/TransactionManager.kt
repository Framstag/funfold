package com.framstag.funfold.transaction

import com.framstag.funfold.annotation.PotentialIO

interface TransactionManager {
    @PotentialIO
    fun getTransaction():Transaction

    // TODO: Add return value?
    @PotentialIO
    fun execute(transactedCode: () -> Unit) {
        val transaction = getTransaction()

        try {
            transactedCode()

            transaction.commit()
        }
        catch (e: Exception) {
            transaction.rollback()

            throw e
        }
    }

}
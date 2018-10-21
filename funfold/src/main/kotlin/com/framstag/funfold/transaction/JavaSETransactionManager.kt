package com.framstag.funfold.transaction

class JavaSETransactionManager : TransactionManager {
    companion object {
        /*
            The thread local object stores the current top level transaction
         */
        private val threadLocal = ThreadLocal<JavaSETransaction>()

        fun getCurrentTransaction():JavaSETransaction? {
            return threadLocal.get()
        }

        fun clearTransaction() {
            threadLocal.set(null)
        }

    }

    override fun getTransaction(): Transaction {
        val currentTransaction = threadLocal.get()

        val newTransaction = JavaSETransaction(currentTransaction)

        if (currentTransaction == null) {
            threadLocal.set(newTransaction)
        }

        return newTransaction
    }
}
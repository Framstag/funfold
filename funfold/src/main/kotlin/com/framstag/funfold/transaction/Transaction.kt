package com.framstag.funfold.transaction

interface Transaction {
    fun commit()
    fun rollback()
}
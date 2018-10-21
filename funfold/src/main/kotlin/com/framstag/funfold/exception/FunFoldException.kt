package com.framstag.funfold.exception

open class FunFoldException: Exception {
    constructor(message: String):super(message)
    constructor(message: String, t: Throwable):super(message,t)
    constructor(t: Throwable):super(t)
}
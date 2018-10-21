package com.framstag.funfold.serialisation

interface Serializer {
    fun <A> toString(o : A):String
    fun <A >fromString(value: String, targetClassName: String):A
}
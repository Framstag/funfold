package com.framstag.funfold.demo.addressbook.serialisation

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Klaxon
import com.beust.klaxon.Parser
import com.framstag.funfold.serialisation.Serializer

class JsonSerializer: Serializer {
    override fun <A> toString(o: A): String {
        return Klaxon().toJsonString(o as Any)
    }

    override fun <A> fromString(value: String, targetClassName: String): A {
        val parser = Parser.default()

        val jsonObject = parser.parse(StringBuilder(value)) as JsonObject

        val eventClass = Class.forName(targetClassName)

        @Suppress("UNCHECKED_CAST")
        return Klaxon().fromJsonObject(jsonObject,eventClass.javaClass, eventClass.kotlin) as A
    }
}
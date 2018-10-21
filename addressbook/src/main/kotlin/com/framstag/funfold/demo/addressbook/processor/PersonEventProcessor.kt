package com.framstag.funfold.demo.addressbook.processor

import com.framstag.funfold.demo.addressbook.PersonCreatedEvent
import com.framstag.funfold.demo.addressbook.PersonMarriedEvent
import mu.KotlinLogging

val logger = KotlinLogging.logger("com.framstag.addressbook.processor")

fun onPersonCreatedEventHandler(event : PersonCreatedEvent) {
    logger.info("Processed ${event::class.java.name}")
}

fun onPersonMarriedEventHandler(event : PersonMarriedEvent) {
    logger.info("Processed ${event::class.java.name}")
}
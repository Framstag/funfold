package com.framstag.funfold.eventsourcing

import com.framstag.funfold.cqrs.Event

/**
 * Context information given to the event sourced comand handler.
 */
class CommandContext(
    val aggregateId: String,
    val aggregateVersion: Long
) {
    var event: Event? = null
}
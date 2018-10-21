package com.framstag.funfold.eventsourcing

/**
 * The event sourced command handler has to extract below information from the command.
 */
data class AggregateInfo(val aggregateId: String, val aggregateVersion: Long?)
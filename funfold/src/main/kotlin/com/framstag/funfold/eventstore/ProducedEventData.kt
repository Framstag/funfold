package com.framstag.funfold.eventstore

import com.framstag.funfold.cqrs.Event

data class ProducedEventData<E : Event>(
    val aggregateName: String,
    val aggregateId: String,
    val version: Long,
    val event: E
)
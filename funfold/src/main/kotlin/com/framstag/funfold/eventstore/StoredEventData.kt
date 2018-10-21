package com.framstag.funfold.eventstore

import com.framstag.funfold.cqrs.Event

data class StoredEventData<E : Event>(
    val serial: Long,
    val aggregateName: String,
    val aggregateId: String,
    val hash: Int,
    val version: Long,
    val event: E
)
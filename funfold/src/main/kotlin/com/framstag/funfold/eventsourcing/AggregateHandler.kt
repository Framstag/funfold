package com.framstag.funfold.eventsourcing

/**
 * AggregateHandler are called in the context of building up an aggregate based on past events.
 */
typealias AggregateHandler<A,E> = (aggregate: A, event: E) -> A

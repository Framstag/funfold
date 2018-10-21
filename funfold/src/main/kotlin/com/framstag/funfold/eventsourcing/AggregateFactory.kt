package com.framstag.funfold.eventsourcing

/**
 * Factory method that gets called to create a new instance of an aggregate
 */
typealias AggregateFactory<A> = () -> A

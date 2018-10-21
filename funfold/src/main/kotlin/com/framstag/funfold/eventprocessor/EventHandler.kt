package com.framstag.funfold.eventprocessor

/**
 * An EventHandler can be used to process events after they have been created
 * by processing an command and having it stored into the event store.
 *
 * EventHandler are called in the context of a event processor.
 */
typealias EventHandler<E> = (event: E) -> Unit

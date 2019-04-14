package com.framstag.funfold.eventprocessor

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Event

/**
 * An EventProcessor is an object that reliably processes events from the event store with
 * at-least-once semantic. A EventProcessor holds it position in the event store and thus makes sure, that
 * every message is processed once successfully (but may be processed multiple times unsuccessfully).
 *
 * In rare cases (during fail over and repartitioning) it may be possible that the handler is called for a event that
 * was already processed before. Ths handlers should be idempotent.
 * Features:
 * - A EventProcessor has a global unique id.
 * - A EventProcessor distributes its load into a number of partitions. Each partition is processed separately and
 *   has its own partition position.
 * - The EventProcessor supplies a hash method that decides which partition an event belongs to. Events with the
 *   same hash are processed sequentially in order.
 * - A processor should be instantiated for each OS process once.
 */
interface EventProcessor {
    fun id():EventProcessorInstanceId

    @PotentialIO
    fun <E: Event>dispatch(event : E) {
    }
}
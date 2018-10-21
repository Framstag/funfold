package com.framstag.funfold.eventsourcing

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Aggregate
import com.framstag.funfold.cqrs.Command
import com.framstag.funfold.cqrs.CommandResponse
import java.util.concurrent.CompletableFuture

/**
 * Interface for all event sourced command handlers to implement.
 *
 * TODO: We would like al handlers to be simple functions, thus we have to extract getAggregateInfo..
 */
interface EventSourcedCommandHandler<in A : Aggregate, in C : Command, CR : CommandResponse> {
    fun getAggregateInfo(command: C): AggregateInfo

    @PotentialIO
    fun execute(aggregate: A, command: C, context: CommandContext): CompletableFuture<CR>
}
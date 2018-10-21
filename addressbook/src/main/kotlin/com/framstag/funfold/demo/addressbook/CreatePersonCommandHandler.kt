package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.eventsourcing.CommandContext
import com.framstag.funfold.eventsourcing.EventSourcedCommandHandler
import com.framstag.funfold.eventsourcing.AggregateInfo
import java.util.concurrent.CompletableFuture

class CreatePersonCommandHandler :
    EventSourcedCommandHandler<Person, CreatePersonCommand, CreatePersonCommandResponse> {
    override fun getAggregateInfo(command: CreatePersonCommand): AggregateInfo {
        return AggregateInfo(command.id, null)
    }

    override fun execute(
        aggregate: Person,
        command: CreatePersonCommand,
        context: CommandContext
    ): CompletableFuture<CreatePersonCommandResponse> {
        aggregate.setFirstNameAndSurename(command.firstname, command.surename)

        context.event = PersonCreatedEvent(command.firstname, command.surename)

        return CompletableFuture.completedFuture(
            CreatePersonCommandResponse(
                context.aggregateId,
                context.aggregateVersion
            )
        )
    }
}
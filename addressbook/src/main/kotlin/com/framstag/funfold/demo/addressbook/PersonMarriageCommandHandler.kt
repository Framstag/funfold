package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.eventsourcing.CommandContext
import com.framstag.funfold.eventsourcing.EventSourcedCommandHandler
import com.framstag.funfold.eventsourcing.AggregateInfo
import java.util.concurrent.CompletableFuture

class PersonMarriageCommandHandler :
    EventSourcedCommandHandler<Person, PersonMarriageCommand, PersonMarriageCommandResponse> {
    override fun getAggregateInfo(command: PersonMarriageCommand): AggregateInfo {
        return AggregateInfo(command.id, command.version)
    }

    override fun execute(
        aggregate: Person,
        command: PersonMarriageCommand,
        context: CommandContext
    ): CompletableFuture<PersonMarriageCommandResponse> {
        context.event = PersonMarriedEvent(command.surename)

        return CompletableFuture.completedFuture(
            PersonMarriageCommandResponse(
                context.aggregateId,
                context.aggregateVersion
            )
        )
    }
}
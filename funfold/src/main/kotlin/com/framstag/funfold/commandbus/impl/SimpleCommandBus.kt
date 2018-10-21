package com.framstag.funfold.commandbus.impl

import com.framstag.funfold.commandbus.CommandBus
import com.framstag.funfold.commandbus.CommandDispatcher
import com.framstag.funfold.cqrs.*
import mu.KLogging
import java.util.concurrent.CompletableFuture

/**
 * Simple command bus implementation that uses a passed command dispatcher instance to find a command handler
 * for the given command. It returns the command response in case a command is found.
 * The command bus uses a command dispatcher to do the actual work.
 */
class SimpleCommandBus(private val dispatcher: CommandDispatcher) :
    CommandBus {
    companion object : KLogging()

    override fun <C : Command, CR : CommandResponse> execute(command: C): CompletableFuture<CR> {
        return dispatcher.execute(command)
    }
}
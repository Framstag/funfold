package com.framstag.funfold.commandbus

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.commandbus.impl.SimpleCommandBus
import com.framstag.funfold.cqrs.*
import com.framstag.funfold.exception.ReconfigurationException
import com.framstag.funfold.exception.NoHandlerException
import java.util.concurrent.CompletableFuture

/**
 * The command dispatcher has a internal registry of command handlers for commands. It supplies an execute method
 * that delegates execution of the command to the corresponding command handler - if found.
 */
class CommandDispatcher {
    private val commandMap: MutableMap<String, CommandHandler<Command, CommandResponse>> = mutableMapOf()

    private fun <C : Command, CR : CommandResponse> getCommandHandler(commandName: String): CommandHandler<C,CR>? {
        @Suppress("UNCHECKED_CAST")
        return commandMap[commandName] as CommandHandler<C, CR>?
    }

    fun <C : Command, CR : CommandResponse> registerCommandHandler(command: Class<C>, handler: CommandHandler<C,CR>) {
        val existingHandler = commandMap[command.name]

        if (existingHandler != null) {
            throw ReconfigurationException("Command ${command.name} already has a handler")
        }

        @Suppress("UNCHECKED_CAST")
        commandMap[command.name] = handler as CommandHandler<Command, CommandResponse>
    }

    @PotentialIO
    fun <C : Command, CR : CommandResponse> execute(command : C): CompletableFuture<CR> {
        val commandName = command::class.java.name

        SimpleCommandBus.logger.info("Executing command $commandName")

        val handler = getCommandHandler<C,CR>(commandName)

        if (handler == null) {
            throw NoHandlerException("No handler for command '$commandName' registered")
        }

        return handler(command)
    }
}
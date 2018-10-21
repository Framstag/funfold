package com.framstag.funfold.commandbus

import com.framstag.funfold.annotation.PotentialIO
import com.framstag.funfold.cqrs.Command
import com.framstag.funfold.cqrs.CommandResponse
import java.util.concurrent.CompletableFuture

/**
 * Interface for a command bus implementation.
 *
 * The command bus allows synchronous and asynchronous execution
 * of commands. The result of a command is a command response object.
 *
 * The command bus makes no assumptions to what happens during command execution. It does
 * not pass or assumes any context. It is up to the command handler itself (using clojures for
 * example) to create the context itself.
 */
interface CommandBus {
    @PotentialIO
    fun <C : Command, CR : CommandResponse> execute(command: C): CompletableFuture<CR>

    @PotentialIO
    fun <C : Command, CR : CommandResponse> executeSync(command: C): CR {
        val result: CompletableFuture<CR> = execute(command)

        return result.get()
    }
}
package com.framstag.funfold.commandbus

import java.util.concurrent.CompletableFuture

/**
 * General CommandHandler that gets called with the command as parameter from within the CommandBus.
 * The command handler returns a CommandResponse which can be empty.
 */
typealias CommandHandler<C,CR> = (command: C) -> CompletableFuture<CR>

package com.framstag.funfold.commandbus

import com.framstag.funfold.cqrs.CommandResponse
import com.framstag.funfold.exception.ReconfigurationException
import com.framstag.funfold.exception.NoHandlerException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CompletableFuture

class CommandDispatcherTest {

    data class TestCommand(val id : String) : com.framstag.funfold.cqrs.Command

    @Test
    fun testRegisteredCommandHandler() {
        val dispatcher = CommandDispatcher()

        dispatcher.registerCommandHandler(TestCommand::class.java) { _: TestCommand ->
            CompletableFuture.completedFuture(defaultCommandResponse)
        }
    }

    @Test
    fun testRegisterTwiceThrowsException() {
        val dispatcher = CommandDispatcher()

        dispatcher.registerCommandHandler(TestCommand::class.java) { _: TestCommand ->
            CompletableFuture.completedFuture(defaultCommandResponse)
        }

        val exception = assertThrows<ReconfigurationException> {
            dispatcher.registerCommandHandler(TestCommand::class.java) { _: TestCommand ->
                CompletableFuture.completedFuture(defaultCommandResponse)
            }
        }

        assertEquals("Command ${TestCommand::class.java.name} already has a handler", exception.message)
    }

    @Test
    fun testRegisteredCommandHandlerCalled() {
        val dispatcher = CommandDispatcher()
        var called = false

        dispatcher.registerCommandHandler(TestCommand::class.java) { _: TestCommand ->
            called = true
            CompletableFuture.completedFuture(defaultCommandResponse)
        }

        val command = TestCommand("1")
        dispatcher.execute<TestCommand, CommandResponse>(command)

        assertTrue(called)
    }

    @Test
    fun testRegisteredCommandPassed() {
        val dispatcher = CommandDispatcher()
        var called = false

        dispatcher.registerCommandHandler(TestCommand::class.java) { command : TestCommand ->
            called = true
            assertEquals("1",command.id)
            CompletableFuture.completedFuture(defaultCommandResponse)
        }

        val command = TestCommand("1")
        dispatcher.execute<TestCommand, CommandResponse>(command)

        assertTrue(called)
    }

    @Test
    fun testThrowsErrorOnUnknownCommand() {
        val dispatcher = CommandDispatcher()

        val command = TestCommand("1")

        val exception = assertThrows<NoHandlerException> {
            dispatcher.execute<TestCommand, CommandResponse>(command)
        }

        assertEquals("No handler for command '${TestCommand::class.java.name}' registered", exception.message)
    }
}
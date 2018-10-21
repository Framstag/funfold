package com.framstag.funfold.cqrs

/**
 *
 * Marker interface all command responses have to implement. Only use to get a
 * minimum typesafe API.
 *
 * TODO: Is CommandResult a better name? The CommandResponse might that the response does not necessarily
 * include the actual result of the command - only some data.
*/
interface CommandResponse
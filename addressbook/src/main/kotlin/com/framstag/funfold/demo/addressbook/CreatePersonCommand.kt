package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.cqrs.Command

data class CreatePersonCommand(val id: String, val firstname: String, val surename: String) : Command
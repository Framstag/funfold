package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.cqrs.CommandResponse

data class CreatePersonCommandResponse(val id: String, val version: Long) :
    CommandResponse
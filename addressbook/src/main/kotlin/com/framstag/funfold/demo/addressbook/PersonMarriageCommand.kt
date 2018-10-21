package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.cqrs.Command

data class PersonMarriageCommand(val id: String, val version : Long, val surename: String?) : Command
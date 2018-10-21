package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.cqrs.Event

data class PersonCreatedEvent(val firstname: String, val surename: String) : Event
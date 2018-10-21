package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.cqrs.Event

data class PersonMarriedEvent(val surename: String?) : Event
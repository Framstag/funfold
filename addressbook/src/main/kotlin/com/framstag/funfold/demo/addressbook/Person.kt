package com.framstag.funfold.demo.addressbook

import com.framstag.funfold.cqrs.Aggregate

enum class PersonStatus {
    SINGLE,
    MARRIED,
    DIVORCED,
    WIDOWED
}

class Person : Aggregate {
    private var firstname: String = ""
    private var surename: String = ""
    private var status = PersonStatus.SINGLE

    fun setFirstNameAndSurename(firstname: String, surename: String) {
        this.firstname = firstname
        this.surename = surename
    }

    fun marriage(surename: String?) {
        if (!(status == PersonStatus.SINGLE || status == PersonStatus.DIVORCED || status == PersonStatus.WIDOWED)) {
            throw IllegalStateException("Precondition for marriage not fulfilled")
        }

        if (surename != null) {
            this.surename = surename
        }

        status = PersonStatus.MARRIED
    }
}

fun personCreatedHandler(person : Person, event : PersonCreatedEvent): Person {
    person.setFirstNameAndSurename(event.firstname,event.surename)

    return person
}

fun personMarriedHandler(person : Person, event : PersonMarriedEvent): Person {
    person.marriage(event.surename)

    return person
}
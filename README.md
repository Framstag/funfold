# About "funfold"

Funfold is a small project to build a simple CQRS/ Event Sourcing
framework in Kotlin.

The goal is not to build the best production ready framework
but to build the framework with the best "design", document
each design decision (in code) and just learn a lot. 

In concrete design goals are:
* Use a pragmatic functional approach.
* Support reactive programming but not require it.
* Separate domain code from CQRS/ES code.
* Do not require Java EE or Spring or another framework in the core
library but allow integration and use of any of these (or other)
frameworks.
* Does not require CDI or similar technics in the core library.
* Supply adapter code in separate modules. 
* Clearly document the purpose of each component.
* Target distributed systems, especially allowing multiple
distributed instances of each building block.
* Do not fix design to tighly on the programming language, 
be able to reuse most of the used modeling and patterns
in another functional programming language.
* Be functional complete and fully tested.

Concretes features planed:
* CommandBus
* InMemory EventStore
* JDBC based event store implementation for relational databases
* At least one non-relational database event store implementation
* Support for CQRS with and without event sourcing
* Support for aggregate snapshots
* Support for in-memory event processor (at most once for local events)
* Support for distributed event processor with at least once semantics
with JDBC state store
* Support for proper transaction handling
* Support for custom serialization   

# Current status

None of the above goals is currently completely reached. The code
is still experimental and we are still "storming" and writing code
to find out the best modelling and APIs.

# Be invited!

If you are interested, participate. Note that the project is likely
more about discussing the "right code" first and then supply an
implementation than actually supplying the
"right code" immediately.
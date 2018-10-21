package com.framstag.funfold.annotation

/**
 * Methods annotated with this annotation may do IO operations and thus should be reactive.
 *
 * The annotation is a signal to check for API correctness and implicitly - if the
 * API is not correct - signal a pending API fix.
 *
 * A method that does potential IO may spend and undefined amount of time and should be reactive.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.SOURCE)
@MustBeDocumented
annotation class PotentialIO
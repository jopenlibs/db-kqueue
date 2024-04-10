package io.github.jopenlibs.dbkqueue.api

import nl.jqno.equalsverifier.EqualsVerifier
import kotlin.test.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class TaskExecutionResultTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(TaskExecutionResult::class.java).verify()
    }
}
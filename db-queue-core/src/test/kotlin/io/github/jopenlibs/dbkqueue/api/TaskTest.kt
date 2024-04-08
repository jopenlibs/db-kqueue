package io.github.jopenlibs.dbkqueue.api

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class TaskTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(Task::class.java).verify()
    }
}
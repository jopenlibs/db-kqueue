package io.github.jopenlibs.dbkqueue.api

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.junit.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class EnqueueParamsTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(io.github.jopenlibs.dbkqueue.api.EnqueueParams::class.java).suppress(Warning.NONFINAL_FIELDS).verify()
    }
}
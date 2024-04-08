package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.Test

/**
 * @author Oleg Kandaurov
 * @since 27.09.2017
 */
class QueueIdTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueId::class.java).verify()
    }
}
package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class QueueConfigTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueConfig::class.java).verify()
    }
}
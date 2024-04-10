package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

/**
 * @author Oleg Kandaurov
 * @since 27.09.2017
 */
class QueueIdTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueId::class.java).verify()
    }
}
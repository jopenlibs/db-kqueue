package io.github.jopenlibs.dbkqueue.api

import io.github.jopenlibs.dbkqueue.config.QueueShardId
import nl.jqno.equalsverifier.EqualsVerifier
import kotlin.test.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class QueueShardIdTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueShardId::class.java).verify()
    }
}
package io.github.jopenlibs.dbkqueue.api

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.Test
import ru.yoomoney.tech.dbqueue.config.QueueShardId

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
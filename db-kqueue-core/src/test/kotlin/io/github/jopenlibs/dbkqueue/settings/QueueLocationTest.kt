package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class QueueLocationTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueLocation::class.java).verify()
    }

    @Test
    fun should_filter_special_chars_in_table_name() {
        assertThat(
            QueueLocation.builder().withQueueId(QueueId("1"))
                .withTableName(" t !@#$%^&*()._+-=1\n;'][{}").build().tableName
        ).isEqualTo("t._1")
    }

    @Test
    fun should_filter_special_chars_in_sequence_name() {
        assertThat(
            QueueLocation.builder().withQueueId(QueueId("1"))
                .withTableName("1")
                .withIdSequence(" s !@#$%^&*()._+-=1\n;'][{}").build().getIdSequence().get()
        ).isEqualTo("s._1")
    }
}
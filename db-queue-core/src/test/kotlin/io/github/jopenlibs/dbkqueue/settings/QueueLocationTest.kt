package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class QueueLocationTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueLocation::class.java).verify()
    }

    @Test
    fun should_filter_special_chars_in_table_name() {
        Assert.assertThat(
            QueueLocation.builder().withQueueId(QueueId("1"))
                .withTableName(" t !@#$%^&*()._+-=1\n;'][{}").build().tableName, CoreMatchers.equalTo("t._1")
        )
    }

    @Test
    fun should_filter_special_chars_in_sequence_name() {
        Assert.assertThat(
            QueueLocation.builder().withQueueId(QueueId("1"))
                .withTableName("1")
                .withIdSequence(" s !@#$%^&*()._+-=1\n;'][{}").build().getIdSequence().get(),
            CoreMatchers.equalTo("s._1")
        )
    }
}
package io.github.jopenlibs.dbkqueue.config

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * @author Oleg Kandaurov
 * @since 16.10.2019
 */
class QueueTableSchemaTest {
    @Test
    fun should_filter_special_chars() {
        val schema = QueueTableSchema.builder()
            .withIdField("qid !@#$%^&*()_+-=1\n;'][{}")
            .withQueueNameField("qn !@#$%^&*()_+-=1\n;'][{}")
            .withPayloadField("pl !@#$%^&*()_+-=1\n;'][{}")
            .withCreatedAtField("ct !@#$%^&*()_+-=1\n;'][{}")
            .withNextProcessAtField("pt !@#$%^&*()_+-=1\n;'][{}")
            .withAttemptField("at !@#$%^&*()_+-=1\n;'][{}")
            .withReenqueueAttemptField("rat !@#$%^&*()_+-=1\n;'][{}")
            .withTotalAttemptField("tat !@#$%^&*()_+-=1\n;'][{}")
            .withExtFields(listOf("tr !@#$%^&*()_+-=1\n;'][{}"))
            .build()
        assertThat(schema.idField).isEqualTo("qid_1")
        assertThat(schema.queueNameField).isEqualTo("qn_1")
        assertThat(schema.payloadField).isEqualTo("pl_1")
        assertThat(schema.createdAtField).isEqualTo("ct_1")
        assertThat(schema.nextProcessAtField).isEqualTo("pt_1")
        assertThat(schema.attemptField).isEqualTo("at_1")
        assertThat(schema.reenqueueAttemptField).isEqualTo("rat_1")
        assertThat(schema.totalAttemptField).isEqualTo("tat_1")
        assertThat(schema.extFields[0]).isEqualTo("tr_1")
    }
}

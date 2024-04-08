package ru.yoomoney.tech.dbqueue.config

import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test

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
        Assert.assertThat(schema.idField, CoreMatchers.equalTo("qid_1"))
        Assert.assertThat(schema.queueNameField, CoreMatchers.equalTo("qn_1"))
        Assert.assertThat(schema.payloadField, CoreMatchers.equalTo("pl_1"))
        Assert.assertThat(schema.createdAtField, CoreMatchers.equalTo("ct_1"))
        Assert.assertThat(schema.nextProcessAtField, CoreMatchers.equalTo("pt_1"))
        Assert.assertThat(schema.attemptField, CoreMatchers.equalTo("at_1"))
        Assert.assertThat(schema.reenqueueAttemptField, CoreMatchers.equalTo("rat_1"))
        Assert.assertThat(schema.totalAttemptField, CoreMatchers.equalTo("tat_1"))
        Assert.assertThat(schema.extFields[0], CoreMatchers.equalTo("tr_1"))
    }
}

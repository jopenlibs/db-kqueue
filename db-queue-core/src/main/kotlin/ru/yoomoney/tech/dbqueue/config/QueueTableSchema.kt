package ru.yoomoney.tech.dbqueue.config

import java.util.*
import java.util.regex.Pattern
import java.util.stream.Collectors

/**
 * Scheme for column names of queue table in the database.
 *
 * @author Oleg Kandaurov
 * @since 05.10.2019
 */
class QueueTableSchema private constructor(
    idField: String,
    queueNameField: String,
    payloadField: String,
    attemptField: String,
    reenqueueAttemptField: String,
    totalAttemptField: String,
    createdAtField: String,
    nextProcessAtField: String,
    extFields: List<String>
) {
    /**
     * Field with a column name for the id.
     *
     * @return Column name.
     */
    val idField: String

    /**
     * Field with a column name for the queue name.
     *
     * @return Column name.
     */
    val queueNameField: String

    /**
     * Field with a column name for task payload.
     * Column maps onto [TaskRecord.getPayload].
     *
     * @return Column name.
     */
    val payloadField: String

    /**
     * Field with a column name for task execution attempts count.
     * Column maps onto [TaskRecord.getAttemptsCount].
     *
     * @return Column name.
     */
    val attemptField: String

    /**
     * Field with a column name for task execution re-enqueue attempts count.
     * Column maps onto [TaskRecord.getReenqueueAttemptsCount].
     *
     * @return Column name.
     */
    val reenqueueAttemptField: String

    /**
     * Field with a column name for task execution total attempts count.
     * Column maps onto [TaskRecord.getTotalAttemptsCount].
     *
     * @return Column name.
     */
    val totalAttemptField: String

    /**
     * Field with a column name for task creation date and time.
     * Column maps onto [TaskRecord.getCreatedAt].
     *
     * @return Column name.
     */
    val createdAtField: String

    /**
     * Field with a column name for task processing date and time (task will be processed after this date).
     *
     * @return Column name.
     */
    val nextProcessAtField: String

    /**
     * Additional list of column names (`TEXT` type),
     * which are mapping onto [TaskRecord.getExtData].
     *
     * @return List of column names.
     */
    val extFields: List<String>

    init {
        this.idField = removeSpecialChars(idField)
        this.queueNameField = removeSpecialChars(queueNameField)
        this.payloadField = removeSpecialChars(payloadField)
        this.attemptField = removeSpecialChars(attemptField)
        this.reenqueueAttemptField = removeSpecialChars(reenqueueAttemptField)
        this.totalAttemptField = removeSpecialChars(totalAttemptField)
        this.createdAtField = removeSpecialChars(createdAtField)
        this.nextProcessAtField = removeSpecialChars(nextProcessAtField)
        this.extFields = extFields.stream().map { value: String -> removeSpecialChars(value) }
            .collect(Collectors.toList())
    }

    /**
     * Builder for [QueueTableSchema] class.
     */
    class Builder {
        private var idField = "id"
        private var queueNameField = "queue_name"
        private var payloadField = "payload"
        private var attemptField = "attempt"
        private var reenqueueAttemptField = "reenqueue_attempt"
        private var totalAttemptField = "total_attempt"
        private var createdAtField = "created_at"
        private var nextProcessAtField = "next_process_at"
        private var extFields: List<String> = ArrayList()

        fun withIdField(idField: String): Builder {
            this.idField = idField
            return this
        }

        fun withQueueNameField(queueNameField: String): Builder {
            this.queueNameField = queueNameField
            return this
        }

        fun withPayloadField(payloadField: String): Builder {
            this.payloadField = payloadField
            return this
        }

        fun withAttemptField(attemptField: String): Builder {
            this.attemptField = attemptField
            return this
        }

        fun withReenqueueAttemptField(reenqueueAttemptField: String): Builder {
            this.reenqueueAttemptField = reenqueueAttemptField
            return this
        }

        fun withTotalAttemptField(totalAttemptField: String): Builder {
            this.totalAttemptField = totalAttemptField
            return this
        }

        fun withCreatedAtField(createdAtField: String): Builder {
            this.createdAtField = createdAtField
            return this
        }

        fun withNextProcessAtField(nextProcessAtField: String): Builder {
            this.nextProcessAtField = nextProcessAtField
            return this
        }

        fun withExtFields(extFields: List<String>): Builder {
            this.extFields = extFields
            return this
        }

        fun build(): QueueTableSchema {
            return QueueTableSchema(
                idField, queueNameField, payloadField, attemptField, reenqueueAttemptField,
                totalAttemptField, createdAtField, nextProcessAtField, extFields
            )
        }
    }

    companion object {
        /**
         * Regexp for SQL injection prevention
         */
        private val DISALLOWED_CHARS: Pattern = Pattern.compile("[^a-zA-Z0-9_]*")

        /**
         * Delete special chars to prevent SQL injection
         *
         * @param value input string
         * @return string without special chars
         */
        private fun removeSpecialChars(value: String): String {
            return DISALLOWED_CHARS.matcher(value).replaceAll("")
        }

        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

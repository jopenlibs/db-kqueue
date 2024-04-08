package io.github.jopenlibs.dbkqueue.settings

import java.util.*
import java.util.regex.Pattern

/**
 * Queue location in the database.
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
class QueueLocation private constructor(
    val queueId: QueueId,
    tableName: String,
    idSequence: String?
) {
    val tableName: String
    private val idSequence: String?

    init {
        this.tableName = DISALLOWED_CHARS.matcher(tableName).replaceAll("")
        this.idSequence = if (idSequence != null) DISALLOWED_CHARS.matcher(idSequence).replaceAll("") else null
    }

    /**
     * Get id sequence name.
     *
     *
     * Use for databases which doesn't have automatically incremented primary keys, for example Oracle 11g
     *
     * @return database sequence name for generating primary key of tasks table.
     */
    fun getIdSequence(): Optional<String> {
        return Optional.ofNullable(idSequence)
    }

    override fun toString(): String {
        return '{'.toString() +
                "id=" + queueId +
                ",table=" + tableName +
                (if (idSequence != null) ",idSequence=$idSequence" else "") +
                '}'
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as QueueLocation
        return tableName == that.tableName && queueId == that.queueId && idSequence == that.idSequence
    }

    override fun hashCode(): Int {
        return Objects.hash(tableName, queueId, idSequence)
    }

    /**
     * A builder for class [QueueLocation].
     */
    class Builder {
        private var tableName: String? = null
        private var queueId: QueueId? = null
        private var idSequence: String? = null

        /**
         * Set table name for queue tasks.
         *
         * @param tableName Table name.
         * @return Reference to the same builder.
         */
        fun withTableName(tableName: String): Builder {
            this.tableName = tableName
            return this
        }

        /**
         * Set queue identifier.
         *
         * @param queueId Queue identifier.
         * @return Reference to the same builder.
         */
        fun withQueueId(queueId: QueueId): Builder {
            this.queueId = queueId
            return this
        }

        /**
         * Set id sequence name.
         *
         * @param idSequence database sequence name for generating primary key of tasks table.
         * @return Reference to the same builder.
         */
        fun withIdSequence(idSequence: String?): Builder {
            this.idSequence = idSequence
            return this
        }

        /**
         * Build queue location object.
         *
         * @return Queue location  object.
         */
        fun build(): QueueLocation {
            return QueueLocation(queueId!!, tableName!!, idSequence)
        }
    }

    companion object {
        /**
         * Regexp for SQL injection prevention
         */
        private val DISALLOWED_CHARS: Pattern = Pattern.compile("[^a-zA-Z0-9_\\.]*")

        /**
         * Create a new builder for queue location.
         *
         * @return A builder for queue location.
         */
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

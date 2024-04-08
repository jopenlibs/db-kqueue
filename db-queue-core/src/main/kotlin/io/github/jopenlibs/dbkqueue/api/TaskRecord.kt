package io.github.jopenlibs.dbkqueue.api

import java.time.ZonedDateTime
import java.util.*

/**
 * Raw database record with task parameters and payload
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
class TaskRecord private constructor(
    /**
     * Get unique (sequence id) identifier of the task.
     *
     * @return task identifier
     */
    val id: Long,
    /**
     * Get raw task payload.
     *
     * @return task payload
     */
    val payload: String?,
    /**
     * Get number of attempts to execute the task, including the current one.
     *
     * @return number of attempts to execute the task.
     */
    val attemptsCount: Long,
    /**
     * Get number of attempts to postpone (re-enqueue) the task.
     *
     * @return number of attempts to postpone (re-enqueue) the task.
     */
    val reenqueueAttemptsCount: Long,
    /**
     * Get sum of all attempts to execute the task,
     * including all task re-enqueue attempts and all failed attempts.
     * <br></br>
     * **This counter should never be reset.**
     *
     * @return sum of all attempts to execute the task
     */
    val totalAttemptsCount: Long,

    /**
     * Date and time when the task was added into the queue.
     */
    val createdAt: ZonedDateTime,
    /**
     * Det date and time of the next task execution.
     */
    val nextProcessAt: ZonedDateTime,
    /**
     * The map of external user-defined parameters,
     * where the key is the column name in the tasks table.
     */
    val extData: Map<String, String>
) {
    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as TaskRecord
        return id == that.id && attemptsCount == that.attemptsCount && reenqueueAttemptsCount == that.reenqueueAttemptsCount && totalAttemptsCount == that.totalAttemptsCount && payload == that.payload && createdAt == that.createdAt && nextProcessAt == that.nextProcessAt && extData == that.extData
    }

    override fun hashCode(): Int {
        return Objects.hash(
            id, payload, attemptsCount, reenqueueAttemptsCount, totalAttemptsCount,
            createdAt, nextProcessAt, extData
        )
    }

    override fun toString(): String {
        return '{'.toString() +
                "id=" + id +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createdAt=" + createdAt +
                ", nextProcessAt=" + nextProcessAt +
                '}'
    }

    /**
     * Builder for the [TaskRecord] class
     */
    class Builder {
        private var id: Long = 0
        private var payload: String? = null
        private var attemptsCount: Long = 0
        private var reenqueueAttemptsCount: Long = 0
        private var totalAttemptsCount: Long = 0

        private var createdAt: ZonedDateTime = ZonedDateTime.now()

        private var nextProcessAt: ZonedDateTime = ZonedDateTime.now()

        private var extData: Map<String, String> = LinkedHashMap()

        fun withCreatedAt(createdAt: ZonedDateTime): Builder {
            this.createdAt = Objects.requireNonNull(createdAt, "createdAt")
            return this
        }

        fun withNextProcessAt(nextProcessAt: ZonedDateTime): Builder {
            this.nextProcessAt = Objects.requireNonNull(nextProcessAt, "nextProcessAt")
            return this
        }

        fun withId(id: Long): Builder {
            this.id = id
            return this
        }

        fun withPayload(payload: String?): Builder {
            this.payload = payload
            return this
        }

        fun withAttemptsCount(attemptsCount: Long): Builder {
            this.attemptsCount = attemptsCount
            return this
        }

        fun withReenqueueAttemptsCount(reenqueueAttemptsCount: Long): Builder {
            this.reenqueueAttemptsCount = reenqueueAttemptsCount
            return this
        }

        fun withTotalAttemptsCount(totalAttemptsCount: Long): Builder {
            this.totalAttemptsCount = totalAttemptsCount
            return this
        }

        fun withExtData(extData: Map<String, String>): Builder {
            this.extData = Objects.requireNonNull(extData)
            return this
        }

        fun build(): TaskRecord {
            return TaskRecord(
                id, payload, attemptsCount, reenqueueAttemptsCount,
                totalAttemptsCount, createdAt, nextProcessAt, extData
            )
        }
    }

    companion object {
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

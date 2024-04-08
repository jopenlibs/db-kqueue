package io.github.jopenlibs.dbkqueue.api

import ru.yoomoney.tech.dbqueue.config.QueueShardId
import java.time.ZonedDateTime
import java.util.*

/**
 * Typed task wrapper with parameters, which is supplied to the [QueueConsumer] task processor
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 10.07.2017
</PayloadT> */
class Task<PayloadT> private constructor(
    val shardId: QueueShardId,
    private val payload: PayloadT?,
    /**
     * Get number of attempts to execute the task, including the current one.
     *
     * @return Number of attempts to execute the task.
     */
    val attemptsCount: Long,
    /**
     * Get number of attempts to postpone (re-enqueue) the task.
     *
     * @return Number of attempts to postpone (re-enqueue) the task.
     */
    val reenqueueAttemptsCount: Long,
    /**
     * Get sum of all attempts to execute the task, including all task re-enqueue attempts and all failed attempts.
     * <br></br>
     * **This counter should never be reset.**
     *
     * @return Sum of all attempts to execute the task.
     */
    val totalAttemptsCount: Long,
    val createdAt: ZonedDateTime,
    val extData: Map<String, String>
) {
    /**
     * Get typed task payload.
     *
     * @return Typed task payload.
     */


    fun payload(): Optional<out PayloadT> {
        return Optional.ofNullable(payload)
    }

    val payloadOrThrow: PayloadT
        /**
         * Get typed task payload or throw [IllegalArgumentException] if not present.
         *
         * @return Typed task payload.
         */
        get() {
            requireNotNull(payload) { "payload is absent" }
            return payload
        }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val task = obj as Task<*>
        return attemptsCount == task.attemptsCount && reenqueueAttemptsCount == task.reenqueueAttemptsCount && totalAttemptsCount == task.totalAttemptsCount && shardId == task.shardId && payload == task.payload && createdAt == task.createdAt && extData == task.extData
    }

    override fun hashCode(): Int {
        return Objects.hash(
            shardId, payload, attemptsCount, reenqueueAttemptsCount,
            totalAttemptsCount, createdAt, extData
        )
    }

    override fun toString(): String {
        return '{'.toString() +
                "shardId=" + shardId +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createdAt=" + createdAt +
                ", payload=" + payload +
                '}'
    }

    /**
     * Builder for the [Task] wrapper.
     *
     * @param <PayloadBuilderT> The type of the payload in the task.
    </PayloadBuilderT> */
    class Builder<PayloadBuilderT>(val shardId: QueueShardId) {
        private var createdAt: ZonedDateTime = ZonedDateTime.now()
        private var payload: PayloadBuilderT? = null
        private var attemptsCount: Long = 0
        private var reenqueueAttemptsCount: Long = 0
        private var totalAttemptsCount: Long = 0

        private var extData: Map<String, String> = LinkedHashMap()

        fun withCreatedAt(createdAt: ZonedDateTime): Builder<PayloadBuilderT> {
            this.createdAt = createdAt
            return this
        }

        fun withPayload(payload: PayloadBuilderT): Builder<PayloadBuilderT> {
            this.payload = payload
            return this
        }

        fun withAttemptsCount(attemptsCount: Long): Builder<PayloadBuilderT> {
            this.attemptsCount = attemptsCount
            return this
        }

        fun withReenqueueAttemptsCount(reenqueueAttemptsCount: Long): Builder<PayloadBuilderT> {
            this.reenqueueAttemptsCount = reenqueueAttemptsCount
            return this
        }

        fun withTotalAttemptsCount(totalAttemptsCount: Long): Builder<PayloadBuilderT> {
            this.totalAttemptsCount = totalAttemptsCount
            return this
        }

        fun withExtData(extData: Map<String, String>): Builder<PayloadBuilderT> {
            this.extData = extData
            return this
        }

        fun build(): Task<PayloadBuilderT?> {
            return Task(
                shardId, payload, attemptsCount, reenqueueAttemptsCount,
                totalAttemptsCount, createdAt, extData
            )
        }
    }

    companion object {
        /**
         * Creates a builder for [Task] objects.
         *
         * @param shardId    An id of shard.
         * @param <PayloadBuilderT> A type of task payload.
         * @return A new instance of the [Builder] builder.
        </PayloadBuilderT> */
        @JvmStatic
        fun <PayloadBuilderT> builder(shardId: QueueShardId): Builder<PayloadBuilderT> {
            return Builder(shardId)
        }
    }
}

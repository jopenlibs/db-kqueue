package ru.yoomoney.tech.dbqueue.api

import ru.yoomoney.tech.dbqueue.config.QueueShardId
import java.util.*

/**
 * Task enqueue result
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
class EnqueueResult(val shardId: QueueShardId, val enqueueId: Long) {
    override fun toString(): String {
        return "EnqueueResult{" +
                "shardId=" + shardId +
                ", enqueueId=" + enqueueId +
                '}'
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as EnqueueResult
        return shardId == that.shardId && enqueueId == that.enqueueId
    }

    override fun hashCode(): Int {
        return Objects.hash(shardId, enqueueId)
    }

    /**
     * Builder for the [EnqueueResult] object.
     */
    class Builder {
        private var shardId: QueueShardId? = null
        private var enqueueId: Long? = null

        /**
         * Set shard identifier of added task
         *
         * @param shardId shard identifier of added task
         * @return Builder
         */
        fun withShardId(shardId: QueueShardId): Builder {
            this.shardId = shardId
            return this
        }

        /**
         * Set identifier (sequence id) of added task
         *
         * @param enqueueId sequence id
         * @return Builder
         */
        fun withEnqueueId(enqueueId: Long): Builder {
            this.enqueueId = enqueueId
            return this
        }

        fun build(): EnqueueResult {
            return EnqueueResult(shardId!!, enqueueId!!)
        }
    }

    companion object {
        /**
         * Creates builder for [EnqueueResult] object
         *
         * @return builder
         */
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

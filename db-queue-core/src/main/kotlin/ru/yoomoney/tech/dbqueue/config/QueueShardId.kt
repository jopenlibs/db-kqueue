package ru.yoomoney.tech.dbqueue.config

import java.util.*

/**
 * Storage for shard information.
 *
 * @author Oleg Kandaurov
 * @since 30.07.2017
 */
class QueueShardId(private val id: String) {
    /**
     * Get shard identifier.
     *
     * @return Shard identifier.
     */
    fun asString(): String {
        return id
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val shardId = obj as QueueShardId
        return id == shardId.id
    }

    override fun hashCode(): Int {
        return Objects.hash(id)
    }

    override fun toString(): String {
        return id
    }
}

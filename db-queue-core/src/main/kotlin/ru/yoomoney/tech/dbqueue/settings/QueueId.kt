package ru.yoomoney.tech.dbqueue.settings

import java.util.*

/**
 * Queue identifier.
 *
 * @author Oleg Kandaurov
 * @since 27.09.2017
 */
class QueueId(private val id: String) {
    /**
     * Get string representation of queue identifier.
     *
     * @return Queue identifier.
     */
    fun asString(): String {
        return id
    }

    override fun toString(): String {
        return id
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val queueId = obj as QueueId
        return id == queueId.id
    }

    override fun hashCode(): Int {
        return Objects.hash(id)
    }
}

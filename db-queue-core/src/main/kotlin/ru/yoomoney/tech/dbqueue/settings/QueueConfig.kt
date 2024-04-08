package ru.yoomoney.tech.dbqueue.settings

import java.util.*

/**
 * Queue configuration with database table location and task processing settings.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
class QueueConfig(
    val location: QueueLocation,
    val settings: QueueSettings
) {
    override fun toString(): String {
        return '{'.toString() +
                "location=" + location +
                ", settings=" + settings +
                '}'
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as QueueConfig
        return location == that.location && settings == that.settings
    }

    override fun hashCode(): Int {
        return Objects.hash(location, settings)
    }
}

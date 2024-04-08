package ru.yoomoney.tech.dbqueue.dao

import ru.yoomoney.tech.dbqueue.api.TaskRecord

/**
 * Database access object to pick up tasks in the queue.
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
interface QueuePickTaskDao {
    /**
     * Pick task from a queue
     *
     * @return task data or null if not found
     */
    suspend fun pickTask(): TaskRecord?
}

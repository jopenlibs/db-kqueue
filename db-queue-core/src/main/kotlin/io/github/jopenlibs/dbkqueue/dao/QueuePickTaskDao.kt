package io.github.jopenlibs.dbkqueue.dao

import io.github.jopenlibs.dbkqueue.api.TaskRecord

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

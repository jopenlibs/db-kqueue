package io.github.jopenlibs.dbkqueue.dao

import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import java.time.Duration

/**
 * Database access object to manage tasks in the queue.
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
interface QueueDao {
    /**
     * Add a new task in the queue for processing.
     *
     * @param location      Queue location.
     * @param enqueueParams Parameters of the task
     * @return Identifier (sequence id) of new inserted task.
     */
    suspend fun enqueue(location: QueueLocation, enqueueParams: EnqueueParams<String?>): Long

    /**
     * Remove (delete) task from the queue.
     *
     * @param location Queue location.
     * @param taskId   Identifier (sequence id) of the task.
     * @return true, if task was deleted from database, false, when task with given id was not found.
     */
    suspend fun deleteTask(location: QueueLocation, taskId: Long): Boolean

    /**
     * Postpone task processing for given time period (current date and time plus execution delay).
     *
     * @param location       Queue location.
     * @param taskId         Identifier (sequence id) of the task.
     * @param executionDelay Task execution delay.
     * @return true, if task was successfully postponed, false, when task was not found.
     */
    suspend fun reenqueue(location: QueueLocation, taskId: Long, executionDelay: Duration): Boolean
}

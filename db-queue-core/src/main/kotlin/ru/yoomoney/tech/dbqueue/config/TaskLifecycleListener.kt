package ru.yoomoney.tech.dbqueue.config

import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.settings.QueueLocation

/**
 * Listener for task processing lifecycle.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
interface TaskLifecycleListener {
    /**
     * Event of task picking from the queue.
     *
     *
     * Triggered when there is a task in the queue, which is ready for processing.
     *
     *
     * Might be useful for monitoring problems with database performance.
     *
     * @param shardId      Shard identifier, which processes the queue.
     * @param location     Queue location.
     * @param taskRecord   Raw task data.
     * @param pickTaskTime Time spent on picking the task from the queue in millis.
     */
    fun picked(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        pickTaskTime: Long
    )

    /**
     * The start event of task processing.
     *
     *
     * Always triggered when task was picked.
     *
     *
     * Might be useful for updating a logging context.
     *
     * @param shardId    Shard identifier, which processes the queue.
     * @param location   Queue location.
     * @param taskRecord Raw task data.
     */
    fun started(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    )

    /**
     * Event for completion of client logic when task processing.
     *
     *
     * Always triggered when task processing has completed successfully.
     *
     *
     * Might be useful for monitoring successful execution of client logic.
     *
     * @param shardId         Shard identifier, which processes the queue.
     * @param location        Queue location.
     * @param taskRecord      Raw task data.
     * @param executionResult Result of task processing.
     * @param processTaskTime Time spent on task processing in millis, without the time for task picking from the queue.
     */
    fun executed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        executionResult: TaskExecutionResult,
        processTaskTime: Long
    )

    /**
     * Event for completion the task execution in the queue.
     *
     *
     * Always triggered when task was picked up for processing.
     * Called even after [.crashed].
     *
     *
     * Might be useful for recovery of initial logging context state.
     *
     * @param shardId    Shard identifier, which processes the queue.
     * @param location   Queue location.
     * @param taskRecord Raw task data.
     */
    fun finished(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    )

    /**
     * Event for abnormal queue processing.
     *
     *
     * Triggered when unexpected error occurs during task processing.
     *
     *
     * Might be useful for tracking and monitoring errors in the system.
     *
     * @param shardId    Shard identifier, which processes the queue.
     * @param location   Queue location.
     * @param taskRecord Raw task data.
     * @param exc        An error caused the crash.
     */
    fun crashed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        exc: Exception?
    )
}

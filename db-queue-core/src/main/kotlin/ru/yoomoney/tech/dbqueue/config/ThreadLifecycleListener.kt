package ru.yoomoney.tech.dbqueue.config

import ru.yoomoney.tech.dbqueue.settings.QueueLocation

/**
 * Listener for task processing thread in the queue.
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
interface ThreadLifecycleListener {
    /**
     * Start of the task processing in the queue.
     *
     *
     * Always called.
     *
     *
     * Might be useful for setting values in the logging context or change thread name.
     *
     * @param shardId  Shard identifier, which processes the queue.
     * @param location Queue location.
     */
    fun started(shardId: QueueShardId, location: QueueLocation)

    /**
     * Thread was executed and finished processing.
     *
     *
     * Called when normal end of task processing.
     *
     *
     * Might be useful for measuring performance of the queue.
     *
     * @param shardId        Shard identifier, which processes the queue.
     * @param location       Queue location.
     * @param taskProcessed  Attribute that task was taken and processed, no tasks for processing otherwise.
     * @param threadBusyTime Time in millis of the thread was running active before sleep.
     */
    fun executed(shardId: QueueShardId?, location: QueueLocation?, taskProcessed: Boolean, threadBusyTime: Long)

    /**
     * End of the task processing lifecycle and start of the new one.
     *
     *
     * Always called, even after [.crashed].
     *
     *
     * Might be useful for logging context return or move the thread to the initial state.
     *
     * @param shardId  Shard identifier, which processes the queue.
     * @param location Queue location.
     */
    fun finished(shardId: QueueShardId, location: QueueLocation)

    /**
     * Queue failed with fatal error.
     *
     *
     * Client code cannot trigger that method call,
     * this method is called when task picking crashed.
     *
     *
     * Might be useful for logging and monitoring.
     *
     * @param shardId  Shard identifier, which processes the queue.
     * @param location Queue location.
     * @param exc      An error caused the crash.
     */
    fun crashed(shardId: QueueShardId, location: QueueLocation, exc: Throwable?)
}

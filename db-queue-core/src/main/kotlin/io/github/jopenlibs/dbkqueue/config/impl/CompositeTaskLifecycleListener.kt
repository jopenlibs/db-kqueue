package io.github.jopenlibs.dbkqueue.config.impl

import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import java.util.*
import java.util.function.Consumer

/**
 * Composite listener. It allows combining several listeners into one.
 *
 * Listeners for picked and started events are executed in straight order.
 * Listeners for executed, finished and crashed events are executed in reverse order.
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
class CompositeTaskLifecycleListener(private val listeners: List<TaskLifecycleListener>) : TaskLifecycleListener {
    private val reverseListeners: List<TaskLifecycleListener?> = ArrayList(listeners)

    /**
     * Constructor
     *
     * @param listeners task listeners
     */
    init {
        Collections.reverse(reverseListeners)
    }

    override fun picked(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        pickTaskTime: Long
    ) {
        listeners.forEach(Consumer { l: TaskLifecycleListener? ->
            l!!.picked(
                shardId,
                location,
                taskRecord,
                pickTaskTime
            )
        })
    }

    override fun started(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    ) {
        listeners.forEach(Consumer { l: TaskLifecycleListener? -> l!!.started(shardId, location, taskRecord) })
    }

    override fun executed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        executionResult: TaskExecutionResult,
        processTaskTime: Long
    ) {
        reverseListeners.forEach(Consumer { l: TaskLifecycleListener? ->
            l!!.executed(
                shardId,
                location,
                taskRecord,
                executionResult,
                processTaskTime
            )
        })
    }

    override fun finished(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    ) {
        reverseListeners.forEach(Consumer { l: TaskLifecycleListener? -> l!!.finished(shardId, location, taskRecord) })
    }

    override fun crashed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        exc: Exception?
    ) {
        reverseListeners.forEach(Consumer { l: TaskLifecycleListener? ->
            l!!.crashed(
                shardId,
                location,
                taskRecord,
                exc
            )
        })
    }
}

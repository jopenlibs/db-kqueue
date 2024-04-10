package io.github.jopenlibs.dbkqueue.config.impl

import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.settings.QueueLocation

/**
 * Empty listener for task processing lifecycle.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
class NoopTaskLifecycleListener : TaskLifecycleListener {
    override fun picked(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        pickTaskTime: Long
    ) {
    }

    override fun started(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    ) {
    }

    override fun executed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        executionResult: TaskExecutionResult,
        processTaskTime: Long
    ) {
    }

    override fun finished(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    ) {
    }

    override fun crashed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        exc: Exception?
    ) {
    }

    companion object {
        val instance: NoopTaskLifecycleListener = NoopTaskLifecycleListener()
    }
}

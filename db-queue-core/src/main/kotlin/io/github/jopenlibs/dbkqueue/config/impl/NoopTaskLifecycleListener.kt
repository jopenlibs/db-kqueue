package io.github.jopenlibs.dbkqueue.config.impl

import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.settings.QueueLocation

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

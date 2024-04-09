package io.github.jopenlibs.dbkqueue.stub

import io.github.jopenlibs.dbkqueue.api.Task
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.settings.QueueConfig

/**
 * @author Oleg Kandaurov
 * @since 14.10.2019
 */
class NoopQueueConsumer(queueConfig: QueueConfig) : io.github.jopenlibs.dbkqueue.stub.StringQueueConsumer(queueConfig) {
    override fun <T> execute(task: Task<T>): TaskExecutionResult {
        return finish()
    }
}

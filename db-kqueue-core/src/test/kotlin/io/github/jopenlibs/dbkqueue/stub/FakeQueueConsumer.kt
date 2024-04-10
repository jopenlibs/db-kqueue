package io.github.jopenlibs.dbkqueue.stub

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.api.Task
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskPayloadTransformer
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import java.util.function.Function

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class FakeQueueConsumer(
    override val queueConfig: QueueConfig,
    override val payloadTransformer: TaskPayloadTransformer<String>,
    private val execFunc: Function<Task<String>, TaskExecutionResult>
) : QueueConsumer<String> {
    override fun <T> execute(task: Task<T>): TaskExecutionResult {
        return execFunc.apply(task as Task<String>)
    }
}

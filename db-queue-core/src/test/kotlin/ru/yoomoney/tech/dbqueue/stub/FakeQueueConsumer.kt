package ru.yoomoney.tech.dbqueue.stub

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.api.Task
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
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

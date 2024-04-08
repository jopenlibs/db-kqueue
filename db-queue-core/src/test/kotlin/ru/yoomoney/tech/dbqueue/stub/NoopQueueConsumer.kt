package ru.yoomoney.tech.dbqueue.stub

import ru.yoomoney.tech.dbqueue.api.Task
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult.Companion.finish
import ru.yoomoney.tech.dbqueue.settings.QueueConfig

/**
 * @author Oleg Kandaurov
 * @since 14.10.2019
 */
class NoopQueueConsumer(queueConfig: QueueConfig) : StringQueueConsumer(queueConfig) {
    override fun <T> execute(task: Task<T>): TaskExecutionResult {
        return finish()
    }
}

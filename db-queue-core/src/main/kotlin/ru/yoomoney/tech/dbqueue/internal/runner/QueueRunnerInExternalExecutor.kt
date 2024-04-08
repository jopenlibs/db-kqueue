package ru.yoomoney.tech.dbqueue.internal.runner

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode
import java.util.concurrent.Executor

/**
 * Исполнитель задач очереди в режиме
 * [ProcessingMode.USE_EXTERNAL_EXECUTOR]
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
internal class QueueRunnerInExternalExecutor(
    taskPicker: TaskPicker,
    taskProcessor: TaskProcessor,
    externalExecutor: Executor
) : QueueRunner {
    private val baseQueueRunner = BaseQueueRunner(taskPicker, taskProcessor, externalExecutor)

    override suspend fun runQueue(queueConsumer: QueueConsumer<Any?>): QueueProcessingStatus {
        return baseQueueRunner.runQueue(queueConsumer)
    }
}

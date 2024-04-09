package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.internal.processing.QueueProcessingStatus
import io.github.jopenlibs.dbkqueue.internal.processing.TaskPicker
import io.github.jopenlibs.dbkqueue.internal.processing.TaskProcessor
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

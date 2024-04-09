package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.internal.processing.QueueProcessingStatus
import io.github.jopenlibs.dbkqueue.internal.processing.TaskPicker
import io.github.jopenlibs.dbkqueue.internal.processing.TaskProcessor

/**
 * Исполнитель задач очереди в режиме
 * [ProcessingMode.SEPARATE_TRANSACTIONS]
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
internal class QueueRunnerInSeparateTransactions(
    taskPicker: TaskPicker,
    taskProcessor: TaskProcessor
) : QueueRunner {
    private val baseQueueRunner = BaseQueueRunner(taskPicker, taskProcessor) { obj: Runnable -> obj.run() }

    override suspend fun runQueue(queueConsumer: QueueConsumer<Any?>): QueueProcessingStatus {
        return baseQueueRunner.runQueue(queueConsumer)
    }
}

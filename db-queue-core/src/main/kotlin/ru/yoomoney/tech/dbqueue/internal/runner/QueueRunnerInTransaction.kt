package ru.yoomoney.tech.dbqueue.internal.runner

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode

/**
 * Исполнитель задач очереди в режиме
 * [ProcessingMode.WRAP_IN_TRANSACTION]
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
internal class QueueRunnerInTransaction(
    taskPicker: TaskPicker,
    taskProcessor: TaskProcessor,
    private val queueShard: QueueShard<*>
) : QueueRunner {
    private val baseQueueRunner = BaseQueueRunner(taskPicker, taskProcessor) { obj: Runnable -> obj.run() }

    override suspend fun runQueue(queueConsumer: QueueConsumer<Any?>): QueueProcessingStatus {
        return queueShard.databaseAccessLayer
            .transact { baseQueueRunner.runQueue(queueConsumer) }
    }
}
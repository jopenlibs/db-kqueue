package io.github.jopenlibs.dbkqueue.internal.runner

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode

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

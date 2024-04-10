package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.internal.processing.QueueProcessingStatus
import io.github.jopenlibs.dbkqueue.internal.processing.TaskPicker
import io.github.jopenlibs.dbkqueue.internal.processing.TaskProcessor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import java.util.concurrent.Executor

/**
 * Базовая реализация обработчика задач очереди
 *
 * @author Oleg Kandaurov
 * @since 27.08.2017
 */
class BaseQueueRunner internal constructor(
    private val taskPicker: TaskPicker,
    private val taskProcessor: TaskProcessor,
    private val executor: Executor
) : QueueRunner {
    private val scope = CoroutineScope(executor.asCoroutineDispatcher())

    override suspend fun runQueue(queueConsumer: QueueConsumer<Any?>): QueueProcessingStatus {
        val taskRecord = taskPicker.pickTask() ?: return QueueProcessingStatus.SKIPPED

        scope.launch {
            taskProcessor.processTask(queueConsumer, taskRecord)
        }

        return QueueProcessingStatus.PROCESSED
    }
}

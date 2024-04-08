package ru.yoomoney.tech.dbqueue.internal.runner

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider.SystemMillisTimeProvider
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor
import ru.yoomoney.tech.dbqueue.internal.processing.TaskResultHandler
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode

/**
 * Интерфейс обработчика пула задач очереди
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
interface QueueRunner {
    /**
     * Единократно обработать заданную очередь
     *
     * @param queueConsumer очередь для обработки
     * @return тип результата выполнения задачи
     */
    suspend fun runQueue(queueConsumer: QueueConsumer<Any?>): QueueProcessingStatus

    /**
     * Фабрика исполнителей задач в очереди
     */
    object Factory {
        /**
         * Создать исполнителя задач очереди
         *
         * @param queueConsumer         очередь обработки задач
         * @param queueShard            шард, на котором будут запущен consumer
         * @param taskLifecycleListener слушатель процесса обработки задач
         * @return инстанс исполнителя задач
         */
        fun create(
            queueConsumer: QueueConsumer<*>,
            queueShard: QueueShard<*>,
            taskLifecycleListener: TaskLifecycleListener
        ): QueueRunner {
            val queueSettings = queueConsumer.queueConfig.settings
            val queueLocation = queueConsumer.queueConfig.location

            val queuePickTaskDao = queueShard.databaseAccessLayer.createQueuePickTaskDao(
                queueLocation,
                queueSettings.failureSettings
            )

            val taskPicker = TaskPicker(
                queueShard, queueLocation, taskLifecycleListener,
                SystemMillisTimeProvider(), queuePickTaskDao
            )

            val taskResultHandler = TaskResultHandler(
                queueLocation,
                queueShard, queueSettings.reenqueueSettings
            )

            val taskProcessor = TaskProcessor(
                queueShard, taskLifecycleListener,
                SystemMillisTimeProvider(), taskResultHandler
            )

            val processingMode = queueSettings.processingSettings.processingMode
            when (processingMode) {
                ProcessingMode.SEPARATE_TRANSACTIONS -> return QueueRunnerInSeparateTransactions(
                    taskPicker,
                    taskProcessor
                )

                ProcessingMode.WRAP_IN_TRANSACTION -> return QueueRunnerInTransaction(
                    taskPicker,
                    taskProcessor,
                    queueShard
                )

                ProcessingMode.USE_EXTERNAL_EXECUTOR -> {
                    val executor = queueConsumer.executor()
                    return QueueRunnerInExternalExecutor(taskPicker, taskProcessor,
                        executor.orElseThrow {
                            IllegalArgumentException(
                                "Executor is empty. " +
                                        "You must provide QueueConsumer#getExecutor in ProcessingMode#USE_EXTERNAL_EXECUTOR"
                            )
                        })
                }

                else -> throw IllegalStateException("unknown processing mode: $processingMode")
            }
        }
    }
}

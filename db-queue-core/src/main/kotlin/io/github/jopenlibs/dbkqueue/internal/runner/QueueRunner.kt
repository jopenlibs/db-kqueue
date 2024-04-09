package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.internal.processing.MillisTimeProvider
import io.github.jopenlibs.dbkqueue.internal.processing.QueueProcessingStatus
import io.github.jopenlibs.dbkqueue.internal.processing.TaskPicker
import io.github.jopenlibs.dbkqueue.internal.processing.TaskProcessor
import io.github.jopenlibs.dbkqueue.internal.processing.TaskResultHandler
import io.github.jopenlibs.dbkqueue.settings.ProcessingMode

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
                MillisTimeProvider.SystemMillisTimeProvider(), queuePickTaskDao
            )

            val taskResultHandler = TaskResultHandler(
                queueLocation,
                queueShard, queueSettings.reenqueueSettings
            )

            val taskProcessor = TaskProcessor(
                queueShard, taskLifecycleListener,
                MillisTimeProvider.SystemMillisTimeProvider(), taskResultHandler
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

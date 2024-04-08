package io.github.jopenlibs.dbkqueue.internal.processing

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.api.Task
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener

/**
 * Обработчик выбранной задачи
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
class TaskProcessor(
    private val queueShard: QueueShard<*>,
    private val taskLifecycleListener: TaskLifecycleListener,
    private val millisTimeProvider: MillisTimeProvider,
    private val taskResultHandler: TaskResultHandler
) {
    /**
     * Передать выбранную задачу в клиентский код на выполнение и обработать результат
     *
     * @param queueConsumer очередь
     * @param taskRecord    запись на обработку
     */
    suspend fun processTask(queueConsumer: QueueConsumer<*>, taskRecord: TaskRecord) {
        try {
            taskLifecycleListener.started(
                queueShard.shardId, queueConsumer.queueConfig.location,
                taskRecord
            )
            val processTaskStarted = millisTimeProvider.millis
            val payload = queueConsumer.payloadTransformer.toObject(taskRecord.payload)
            val task: Task<*> = Task.builder<Any?>(queueShard.shardId)
                .withCreatedAt(taskRecord.createdAt)
                .withPayload(payload)
                .withAttemptsCount(taskRecord.attemptsCount)
                .withReenqueueAttemptsCount(taskRecord.reenqueueAttemptsCount)
                .withTotalAttemptsCount(taskRecord.totalAttemptsCount)
                .withExtData(taskRecord.extData)
                .build()

            val executionResult = queueConsumer.execute(task)

            taskLifecycleListener.executed(
                queueShard.shardId, queueConsumer.queueConfig.location,
                taskRecord,
                executionResult, millisTimeProvider.millis - processTaskStarted
            )
            taskResultHandler.handleResult(taskRecord, executionResult)
        } catch (exc: Exception) {
            taskLifecycleListener.crashed(
                queueShard.shardId, queueConsumer.queueConfig.location,
                taskRecord, exc
            )
        } finally {
            taskLifecycleListener.finished(
                queueShard.shardId, queueConsumer.queueConfig.location,
                taskRecord
            )
        }
    }
}

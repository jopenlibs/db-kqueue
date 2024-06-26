package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.ThreadLifecycleListener
import io.github.jopenlibs.dbkqueue.internal.runner.QueueRunner

/**
 * Цикл обработки задачи в очереди.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
class QueueTaskPoller(
    private val threadLifecycleListener: ThreadLifecycleListener,
    private val millisTimeProvider: MillisTimeProvider
) {
    /**
     * Запустить цикл обработки задач в очереди
     *
     * @param queueLoop     стратегия выполнения цикла
     * @param shardId       идентификатор шарда, на котором происходит обработка
     * @param queueConsumer выполняемая очередь
     * @param queueRunner   исполнитель очереди
     */
    suspend fun start(
        queueLoop: QueueLoop,
        shardId: QueueShardId,
        queueConsumer: QueueConsumer<Any?>,
        queueRunner: QueueRunner
    ) {
        queueLoop.doRun {
            val pollSettings = queueConsumer.queueConfig.settings.pollSettings
            try {
                val startTime = millisTimeProvider.millis
                threadLifecycleListener.started(shardId, queueConsumer.queueConfig.location)
                val queueProcessingStatus = queueRunner.runQueue(queueConsumer)
                threadLifecycleListener.executed(
                    shardId, queueConsumer.queueConfig.location,
                    queueProcessingStatus != QueueProcessingStatus.SKIPPED,
                    millisTimeProvider.millis - startTime
                )

                when (queueProcessingStatus) {
                    QueueProcessingStatus.SKIPPED -> {
                        queueLoop.doWait(
                            pollSettings.noTaskTimeout,
                            QueueLoop.WaitInterrupt.ALLOW
                        )
                        return@doRun
                    }

                    QueueProcessingStatus.PROCESSED -> {
                        queueLoop.doWait(
                            pollSettings.betweenTaskTimeout,
                            QueueLoop.WaitInterrupt.DENY
                        )
                        return@doRun
                    }

                    else -> throw IllegalStateException("unknown task loop result$queueProcessingStatus")
                }
            } catch (e: Throwable) {
                threadLifecycleListener.crashed(shardId, queueConsumer.queueConfig.location, e)
                queueLoop.doWait(
                    pollSettings.fatalCrashTimeout,
                    QueueLoop.WaitInterrupt.DENY
                )
            } finally {
                threadLifecycleListener.finished(shardId, queueConsumer.queueConfig.location)
            }
        }
    }
}

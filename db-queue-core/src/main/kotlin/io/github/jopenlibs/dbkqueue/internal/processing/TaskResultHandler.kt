package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.settings.ReenqueueSettings
import java.util.*

/**
 * Обработчик результат выполенения задачи
 *
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskResultHandler(
    private val location: QueueLocation,
    private val queueShard: QueueShard<*>,
    reenqueueSettings: ReenqueueSettings
) {
    private var reenqueueRetryStrategy: ReenqueueRetryStrategy

    /**
     * Конструктор
     *
     * @param location          местоположение очереди
     * @param queueShard        шард на котором происходит обработка задачи
     * @param reenqueueSettings настройки переоткладывания задач
     */
    init {
        this.reenqueueRetryStrategy = ReenqueueRetryStrategy.Factory.create(reenqueueSettings)
        reenqueueSettings.registerObserver { oldValue: ReenqueueSettings, newValue: ReenqueueSettings ->
            reenqueueRetryStrategy = ReenqueueRetryStrategy.Factory.create(newValue)
        }
    }

    /**
     * Обработать результат выполнения задачи
     *
     * @param taskRecord      обработанная задача
     * @param executionResult результат обработки
     */
    suspend fun handleResult(taskRecord: TaskRecord, executionResult: TaskExecutionResult) {
        when (executionResult.actionType) {
            TaskExecutionResult.Type.FINISH -> {
                queueShard.databaseAccessLayer.transact {
                    queueShard.databaseAccessLayer.queueDao
                        .deleteTask(location, taskRecord.id)
                }
                return
            }

            TaskExecutionResult.Type.REENQUEUE -> {
                queueShard.databaseAccessLayer.transact {
                    queueShard.databaseAccessLayer.queueDao
                        .reenqueue(location, taskRecord.id,
                            executionResult.getExecutionDelay().orElseGet {
                                reenqueueRetryStrategy.calculateDelay(
                                    taskRecord
                                )
                            })
                }
                return
            }

            TaskExecutionResult.Type.FAIL -> return

            else -> throw IllegalStateException("unknown action type: " + executionResult.actionType)
        }
    }
}

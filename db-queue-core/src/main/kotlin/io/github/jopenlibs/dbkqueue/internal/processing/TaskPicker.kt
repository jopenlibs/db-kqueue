package io.github.jopenlibs.dbkqueue.internal.processing

import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao
import ru.yoomoney.tech.dbqueue.settings.QueueLocation

/**
 * Класс, обеспечивающий выборку задачи из очереди
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
class TaskPicker(
    private val queueShard: QueueShard<*>,
    private val queueLocation: QueueLocation,
    private val taskLifecycleListener: TaskLifecycleListener,
    private val millisTimeProvider: MillisTimeProvider,
    private val pickTaskDao: QueuePickTaskDao
) {
    /**
     * Выбрать задачу из очереди
     *
     * @return задача или null если отсутствует
     */
    suspend fun pickTask(): TaskRecord? {
        val startPickTaskTime = millisTimeProvider.millis
        val taskRecord = queueShard.databaseAccessLayer.transact { pickTaskDao.pickTask() }
        if (taskRecord == null) {
            return null
        }
        taskLifecycleListener.picked(
            queueShard.shardId, queueLocation, taskRecord,
            millisTimeProvider.millis - startPickTaskTime
        )
        return taskRecord
    }
}

package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.dao.QueuePickTaskDao
import io.github.jopenlibs.dbkqueue.settings.QueueLocation

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

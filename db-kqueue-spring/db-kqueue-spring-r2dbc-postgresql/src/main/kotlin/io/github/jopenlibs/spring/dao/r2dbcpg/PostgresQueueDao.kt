package io.github.jopenlibs.spring.dao.r2dbcpg

import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.config.QueueTableSchema
import io.github.jopenlibs.dbkqueue.dao.QueueDao
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.springframework.data.r2dbc.core.R2dbcEntityOperations
import java.time.Duration

/**
 * Database access object to manage tasks in the queue for PostgreSQL database type.
 */
class PostgresQueueDao(
    private val operations: R2dbcEntityOperations,
    private val queueTableSchema: QueueTableSchema
) : QueueDao {

    override suspend fun enqueue(location: QueueLocation, enqueueParams: EnqueueParams<String?>): Long {
        TODO("Not yet implemented")
    }

    override suspend fun deleteTask(location: QueueLocation, taskId: Long): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun reenqueue(location: QueueLocation, taskId: Long, executionDelay: Duration): Boolean {
        TODO("Not yet implemented")
    }


}

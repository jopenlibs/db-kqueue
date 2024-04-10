package io.github.jopenlibs.spring.dao.r2dbcpg

import io.github.jopenlibs.dbkqueue.config.DatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.config.DatabaseDialect
import io.github.jopenlibs.dbkqueue.config.QueueTableSchema
import io.github.jopenlibs.dbkqueue.dao.QueueDao
import io.github.jopenlibs.dbkqueue.dao.QueuePickTaskDao
import io.github.jopenlibs.dbkqueue.settings.FailureSettings
import io.github.jopenlibs.dbkqueue.settings.QueueLocation

/**
 */
class SpringPgR2dbcDatabaseAccessLayer : DatabaseAccessLayer {
    override val queueDao: QueueDao
        get() = TODO("Not yet implemented")

    override fun createQueuePickTaskDao(
        queueLocation: QueueLocation,
        failureSettings: FailureSettings
    ): QueuePickTaskDao {
        TODO("Not yet implemented")
    }

    override suspend fun <ResultT> transact(supplier: suspend () -> ResultT): ResultT {
        TODO("Not yet implemented")
    }

    override val databaseDialect: DatabaseDialect
        get() = DatabaseDialect.POSTGRESQL

    override val queueTableSchema: QueueTableSchema
        get() = QueueTableSchema.builder().build()
}

package io.github.jopenlibs.dbkqueue.stub

import io.github.jopenlibs.dbkqueue.config.DatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.config.DatabaseDialect
import io.github.jopenlibs.dbkqueue.config.QueueTableSchema
import io.github.jopenlibs.dbkqueue.dao.QueueDao
import io.github.jopenlibs.dbkqueue.dao.QueuePickTaskDao
import io.github.jopenlibs.dbkqueue.settings.FailureSettings
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.mockito.Mockito
import org.mockito.kotlin.mock

class StubDatabaseAccessLayer : DatabaseAccessLayer {
    override val queueDao: QueueDao
    override fun createQueuePickTaskDao(
        queueLocation: QueueLocation,
        failureSettings: FailureSettings
    ): QueuePickTaskDao {
        return Mockito.mock(QueuePickTaskDao::class.java)
    }

    override suspend fun <ResultT> transact(supplier: suspend () -> ResultT): ResultT {
        return supplier()
    }

    constructor() {
        this.queueDao = mock()
    }

    constructor(queueDao: QueueDao) {
        this.queueDao = queueDao
    }

    override val databaseDialect: DatabaseDialect
        get() = DatabaseDialect.POSTGRESQL

    override val queueTableSchema: QueueTableSchema
        get() = QueueTableSchema.builder().build()
}

package ru.yoomoney.tech.dbqueue.stub

import org.mockito.Mockito
import org.mockito.kotlin.mock
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema
import ru.yoomoney.tech.dbqueue.dao.QueueDao
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao
import ru.yoomoney.tech.dbqueue.settings.FailureSettings
import ru.yoomoney.tech.dbqueue.settings.QueueLocation

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

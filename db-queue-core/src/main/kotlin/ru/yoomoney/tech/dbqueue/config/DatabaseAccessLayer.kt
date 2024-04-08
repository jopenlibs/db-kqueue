package ru.yoomoney.tech.dbqueue.config

import ru.yoomoney.tech.dbqueue.dao.QueueDao
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao
import ru.yoomoney.tech.dbqueue.settings.FailureSettings
import ru.yoomoney.tech.dbqueue.settings.QueueLocation

/**
 * Interface for interacting with database
 *
 * @author Oleg Kandaurov
 * @since 22.04.2021
 */
interface DatabaseAccessLayer {
    val queueDao: QueueDao

    /**
     * Create an instance of database-specific DAO based on database type and table schema.
     *
     * @param queueLocation   queue location
     * @param failureSettings settings for handling failures
     * @return database-specific DAO instance.
     */
    fun createQueuePickTaskDao(
        queueLocation: QueueLocation,
        failureSettings: FailureSettings
    ): QueuePickTaskDao

    /**
     * Perform an operation in transaction
     *
     * @param <ResultT> result type
     * @param supplier  operation
     * @return result of operation
    </ResultT> */
    suspend fun <ResultT> transact(supplier: suspend () -> ResultT): ResultT

    val databaseDialect: DatabaseDialect

    val queueTableSchema: QueueTableSchema
}

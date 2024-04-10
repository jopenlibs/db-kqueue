package io.github.jopenlibs.spring.dao.r2dbcpg
import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.config.QueueTableSchema
import io.github.jopenlibs.dbkqueue.dao.QueueDao
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.data.r2dbc.core.R2dbcEntityOperations
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.*

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
abstract class QueueDaoTest(
    protected val queueDao: QueueDao,
    protected val tableName: String,
    protected val tableSchema: QueueTableSchema,
    protected val r2dbcOperations: R2dbcEntityOperations,
    protected val transactionTemplate: TransactionTemplate
) {
    @Test
    fun enqueue_should_accept_null_values(): Unit = runBlocking {
        val location: QueueLocation = generateUniqueLocation()
        val enqueueId: Long = queueDao.enqueue(location, EnqueueParams())
        assertThat(enqueueId).isNotEqualTo(0)
    }

    //    @Test
//    fun enqueue_should_save_all_values() {
//        val location: QueueLocation = generateUniqueLocation()
//        val payload = "{}"
//        val executionDelay = Duration.ofHours(1L)
//        val beforeExecution: ZonedDateTime = ZonedDateTime.now()
//        val enqueueId: Long = executeInTransaction {
//            queueDao.enqueue(
//                location, EnqueueParams.create(payload)
//                    .withExecutionDelay(executionDelay)
//            )
//        }
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                val afterExecution: ZonedDateTime = ZonedDateTime.now()
//                Assert.assertThat(rs.next(), equalTo(true))
//                Assert.assertThat(rs.getString(tableSchema.getPayloadField()), equalTo(payload))
//                val nextProcessAt: ZonedDateTime = ZonedDateTime.ofInstant(
//                    rs.getTimestamp(tableSchema.getNextProcessAtField()).toInstant(),
//                    ZoneId.systemDefault()
//                )
//                Assert.assertThat(
//                    nextProcessAt.isAfter(beforeExecution.plus(executionDelay).minus(WINDOWS_OS_DELAY)),
//                    equalTo(true)
//                )
//                Assert.assertThat(
//                    nextProcessAt.isBefore(afterExecution.plus(executionDelay).plus(WINDOWS_OS_DELAY)),
//                    equalTo(true)
//                )
//                val createdAt: ZonedDateTime = ZonedDateTime.ofInstant(
//                    rs.getTimestamp(tableSchema.getCreatedAtField()).toInstant(),
//                    ZoneId.systemDefault()
//                )
//                Assert.assertThat(createdAt.isAfter(beforeExecution.minus(WINDOWS_OS_DELAY)), equalTo(true))
//                Assert.assertThat(createdAt.isBefore(afterExecution.plus(WINDOWS_OS_DELAY)), equalTo(true))
//
//                val reenqueueAttempt: Long = rs.getLong(tableSchema.getReenqueueAttemptField())
//                Assert.assertFalse(rs.wasNull())
//                Assert.assertEquals(0L, reenqueueAttempt)
//                Any()
//            })
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun delete_should_return_false_when_no_deletion() {
//        val location: QueueLocation = generateUniqueLocation()
//        val deleteResult: Boolean = executeInTransaction { queueDao.deleteTask(location, 0L) }
//        Assert.assertThat(deleteResult, equalTo(false))
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun delete_should_return_true_when_deletion_occur() {
//        val location: QueueLocation = generateUniqueLocation()
//        val enqueueId: Long = executeInTransaction { queueDao.enqueue(location, EnqueueParams()) }
//
//        val deleteResult: Boolean = executeInTransaction { queueDao.deleteTask(location, enqueueId) }
//        Assert.assertThat(deleteResult, equalTo(true))
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                Assert.assertThat(rs.next(), equalTo(false))
//                Any()
//            })
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun reenqueue_should_update_next_process_time() {
//        val location: QueueLocation = generateUniqueLocation()
//        val enqueueId: Long = executeInTransaction { queueDao.enqueue(location, EnqueueParams()) }
//
//        val beforeExecution: ZonedDateTime = ZonedDateTime.now()
//        val executionDelay = Duration.ofHours(1L)
//        val reenqueueResult: Boolean = executeInTransaction { queueDao.reenqueue(location, enqueueId, executionDelay) }
//        Assert.assertThat(reenqueueResult, equalTo(true))
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                val afterExecution: ZonedDateTime = ZonedDateTime.now()
//                Assert.assertThat(rs.next(), equalTo(true))
//                val nextProcessAt: ZonedDateTime = ZonedDateTime.ofInstant(
//                    rs.getTimestamp(tableSchema.getNextProcessAtField()).toInstant(),
//                    ZoneId.systemDefault()
//                )
//
//                Assert.assertThat(
//                    nextProcessAt.isAfter(beforeExecution.plus(executionDelay).minus(WINDOWS_OS_DELAY)),
//                    equalTo(true)
//                )
//                Assert.assertThat(
//                    nextProcessAt.isBefore(afterExecution.plus(executionDelay).plus(WINDOWS_OS_DELAY)),
//                    equalTo(true)
//                )
//                Any()
//            })
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun reenqueue_should_reset_attempts() {
//        val location: QueueLocation = generateUniqueLocation()
//        val enqueueId: Long = executeInTransaction { queueDao.enqueue(location, EnqueueParams()) }
//        executeInTransaction {
//            jdbcTemplate.update((("update " + tableName + " set " + tableSchema.getAttemptField()).toString() + "=10 where " + tableSchema.getIdField()).toString() + "=" + enqueueId)
//        }
//
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                Assert.assertThat(rs.next(), equalTo(true))
//                Assert.assertThat(rs.getLong(tableSchema.getAttemptField()), equalTo(10L))
//                Any()
//            })
//
//        val reenqueueResult: Boolean =
//            executeInTransaction { queueDao.reenqueue(location, enqueueId, Duration.ofHours(1L)) }
//
//        Assert.assertThat(reenqueueResult, equalTo(true))
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                Assert.assertThat(rs.next(), equalTo(true))
//                Assert.assertThat(rs.getLong(tableSchema.getAttemptField()), equalTo(0L))
//                Any()
//            })
//    }
//
//    @Test
//    fun reenqueue_should_increment_reenqueue_attempts() {
//        val location: QueueLocation = generateUniqueLocation()
//
//        val enqueueId: Long = executeInTransaction { queueDao.enqueue(location, EnqueueParams()) }
//
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                Assert.assertThat(rs.next(), equalTo(true))
//                Assert.assertThat(rs.getLong(tableSchema.getReenqueueAttemptField()), equalTo(0L))
//                Any()
//            })
//
//        val reenqueueResult: Boolean =
//            executeInTransaction { queueDao.reenqueue(location, enqueueId, Duration.ofHours(1L)) }
//
//        Assert.assertThat(reenqueueResult, equalTo(true))
//        jdbcTemplate.query<Any>(
//            ("select * from " + tableName + " where " + tableSchema.getIdField()).toString() + "=" + enqueueId,
//            ResultSetExtractor<Any> { rs: ResultSet ->
//                Assert.assertThat(rs.next(), equalTo(true))
//                Assert.assertThat(rs.getLong(tableSchema.getReenqueueAttemptField()), equalTo(1L))
//                Any()
//            })
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun reenqueue_should_return_false_when_no_update() {
//        val location: QueueLocation = generateUniqueLocation()
//        val reenqueueResult: Boolean = executeInTransaction { queueDao.reenqueue(location, 0L, Duration.ofHours(1L)) }
//        Assert.assertThat(reenqueueResult, equalTo(false))
//    }
//
    protected fun generateUniqueLocation(): QueueLocation {
        return QueueLocation.builder().withTableName(tableName)
            .withQueueId(QueueId("test-queue-" + UUID.randomUUID())).build()
    }
//
//
//    protected fun executeInTransaction(runnable: Runnable) {
//        transactionTemplate.execute<Any>(object : TransactionCallbackWithoutResult() {
//            protected override fun doInTransactionWithoutResult(status: TransactionStatus) {
//                runnable.run()
//            }
//        })
//    }
//
//    protected fun <T> executeInTransaction(supplier: Supplier<T>): T {
//        return transactionTemplate.execute<T>(TransactionCallback { status: TransactionStatus? -> supplier.get() })
//    }

    companion object {
        /**
         * Some glitches with Windows
         */
        private val WINDOWS_OS_DELAY: Duration = Duration.ofMinutes(1)
    }
}

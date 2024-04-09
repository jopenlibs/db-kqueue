package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.fail
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.reenqueue
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.dao.QueueDao
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.settings.ReenqueueRetryType
import io.github.jopenlibs.dbkqueue.settings.ReenqueueSettings
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.Mockito
import java.time.Duration

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskResultHandlerTest {
    @Test
    fun should_reenqueue_task(): Unit = runBlocking {
        val taskId = 5L
        val reenqueueDelay = Duration.ofMillis(500L)
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()

        val taskRecord = TaskRecord.builder().withId(taskId).build()
        val queueShard = Mockito.mock(QueueShard::class.java)
        val queueDao = Mockito.mock(QueueDao::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer(
                queueDao
            )
        )

        val result = reenqueue(reenqueueDelay)
        val reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build()
        TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord, result)

        Mockito.verify(queueShard, Mockito.times(2)).databaseAccessLayer
        Mockito.verify(queueDao).reenqueue(location, taskId, reenqueueDelay)
    }

    @Test
    fun should_reenqueue_task_with_new_settings(): Unit = runBlocking {
        val taskId = 5L
        val reenqueueDelay = Duration.ofMillis(500L)
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()

        val taskRecord = TaskRecord.builder().withId(taskId).build()
        val queueShard = Mockito.mock(QueueShard::class.java)
        val queueDao = Mockito.mock(QueueDao::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer(
                queueDao
            )
        )

        val reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build()
        TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(
            taskRecord,
            reenqueue(reenqueueDelay)
        )

        Mockito.verify(queueShard, Mockito.times(2)).databaseAccessLayer
        Mockito.verify(queueDao).reenqueue(location, taskId, reenqueueDelay)


        val newReenqueueDelay = Duration.ofMillis(1000L)
        reenqueueSettings.setValue(
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED)
                .withFixedDelay(newReenqueueDelay).build()
        )
        TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(
            taskRecord,
            reenqueue()
        )
        Mockito.verify(queueDao).reenqueue(location, taskId, newReenqueueDelay)
    }

    @Test
    fun should_finish_task(): Unit  = runBlocking {
        val taskId = 5L
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()

        val taskRecord = TaskRecord.builder().withId(taskId).build()
        val queueDao = Mockito.mock(QueueDao::class.java)
        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer(
                queueDao
            )
        )

        val result = finish()

        val reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build()
        TaskResultHandler(location, queueShard, reenqueueSettings)
            .handleResult(taskRecord, result)

        Mockito.verify(queueShard, Mockito.times(2)).databaseAccessLayer
        Mockito.verify(queueDao).deleteTask(location, taskId)
    }

    @Test
    fun should_fail_task_when_no_delay(): Unit  = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()

        val taskRecord = TaskRecord.builder().build()
        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())

        val result = fail()

        val reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build()
        TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord, result)

        Mockito.verifyNoInteractions(queueShard)
    }

    @Test
    fun should_reenqueue_with_retry_strategy_task(): Unit  = runBlocking {
        val taskId = 5L
        val location = QueueLocation.builder()
            .withTableName("testTable")
            .withQueueId(QueueId("testQueue"))
            .build()

        val taskRecord = TaskRecord.builder().withId(taskId).build()

        val queueDao = Mockito.mock(QueueDao::class.java)

        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer(
                queueDao
            )
        )


        val result = reenqueue()
        val reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED)
            .withFixedDelay(Duration.ofSeconds(10L)).build()
        TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord, result)

        Mockito.verify(queueShard, Mockito.times(2)).databaseAccessLayer
        Mockito.verify(queueDao).reenqueue(location, taskId, Duration.ofSeconds(10L))
    }
}
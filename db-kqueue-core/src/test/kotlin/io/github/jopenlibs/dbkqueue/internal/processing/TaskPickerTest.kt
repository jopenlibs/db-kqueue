package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.dao.QueuePickTaskDao
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.FakeMillisTimeProvider
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import kotlin.test.assertNull

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskPickerTest {
    @Test
    fun should_successfully_pick_task(): Unit = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val shardId = QueueShardId("s1")
        val queueShard: QueueShard<*> = mock()
        whenever(queueShard.shardId).thenReturn(shardId)
        val queueConsumer: QueueConsumer<Any?> = mock()
        whenever<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            )
        )
        val pickTaskDao: QueuePickTaskDao = mock()
        whenever(queueShard.databaseAccessLayer)
            .thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())
        val taskRecord = TaskRecord.builder().build()
        whenever(pickTaskDao.pickTask()).thenReturn(taskRecord)
        whenever(queueShard.shardId).thenReturn(shardId)

        val listener: TaskLifecycleListener = mock()
        val millisTimeProvider = spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))

        val pickedTask = TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTask()

        assertThat(pickedTask).isEqualTo(taskRecord)

        verify(millisTimeProvider, times(2)).millis
        verify(queueShard).databaseAccessLayer
        verify(pickTaskDao).pickTask()
        verify(listener).picked(shardId, location, taskRecord, 2L)

    }

    @Test
    fun should_not_notify_when_task_not_picked(): Unit = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueShard: QueueShard<*> = mock()
        val queueConsumer: QueueConsumer<Any?> = mock()
        whenever<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            )
        )
        val pickTaskDao: QueuePickTaskDao = mock()
        whenever(queueShard.databaseAccessLayer)
            .thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())
        whenever(pickTaskDao.pickTask()).thenReturn(null)
        val listener: TaskLifecycleListener = mock()
        val millisTimeProvider = spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))

        val pickedTask = TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTask()

        assertNull(pickedTask)

        verify(millisTimeProvider).millis
        verify(queueShard).databaseAccessLayer
        verify(pickTaskDao).pickTask()
        verifyNoInteractions(listener)
    }

    @Test
    fun should_not_catch_exception(): Unit = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueShard: QueueShard<*> = mock()
        val queueConsumer: QueueConsumer<Any?> = mock()
        whenever<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            )
        )
        val pickTaskDao: QueuePickTaskDao = mock()
        whenever(queueShard.databaseAccessLayer)
            .thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())
        whenever(pickTaskDao.pickTask()).thenThrow(IllegalStateException("fail"))
        val listener: TaskLifecycleListener = mock()
        val millisTimeProvider = spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))

        assertThrows<java.lang.IllegalStateException> {
            TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTask()
        }

        verify(millisTimeProvider).millis
        verify(queueShard).databaseAccessLayer
        verify(pickTaskDao).pickTask()
        verifyNoInteractions(listener)
    }
}
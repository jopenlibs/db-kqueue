package io.github.jopenlibs.dbkqueue.internal.processing

import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import ru.yoomoney.tech.dbqueue.stub.FakeMillisTimeProvider
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.stub.TestFixtures

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskPickerTest {
    @Test
    @Throws(Exception::class)
    fun should_successfully_pick_task() = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val shardId = QueueShardId("s1")
        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.shardId).thenReturn(shardId)
        val queueConsumer = Mockito.mock(QueueConsumer::class.java)
        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            )
        )
        val pickTaskDao = Mockito.mock(QueuePickTaskDao::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())
        val taskRecord = TaskRecord.builder().build()
        Mockito.`when`(pickTaskDao.pickTask()).thenReturn(taskRecord)
        Mockito.`when`(queueShard.shardId).thenReturn(shardId)
        val listener = Mockito.mock(TaskLifecycleListener::class.java)
        val millisTimeProvider = Mockito.spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))

        val pickedTask = TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTask()

        Assert.assertThat(pickedTask, CoreMatchers.equalTo(taskRecord))

        Mockito.verify(millisTimeProvider, Mockito.times(2)).millis
        Mockito.verify(queueShard).databaseAccessLayer
        Mockito.verify(pickTaskDao).pickTask()
        Mockito.verify(listener).picked(shardId, location, taskRecord, 2L)
    }

    @Test
    @Throws(Exception::class)
    fun should_not_notify_when_task_not_picked() = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueShard = Mockito.mock(QueueShard::class.java)
        val queueConsumer = Mockito.mock(QueueConsumer::class.java)
        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            )
        )
        val pickTaskDao = Mockito.mock(QueuePickTaskDao::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())
        Mockito.`when`(pickTaskDao.pickTask()).thenReturn(null)
        val listener = Mockito.mock(TaskLifecycleListener::class.java)
        val millisTimeProvider = Mockito.spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))

        val pickedTask = TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTask()

        Assert.assertThat(pickedTask, CoreMatchers.equalTo(null))

        Mockito.verify(millisTimeProvider).millis
        Mockito.verify(queueShard).databaseAccessLayer
        Mockito.verify(pickTaskDao).pickTask()
        Mockito.verifyNoInteractions(listener)
    }

    @Test(expected = IllegalStateException::class)
    @Throws(Exception::class)
    fun should_not_catch_exception() = runBlocking {
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueShard = Mockito.mock(QueueShard::class.java)
        val queueConsumer = Mockito.mock(QueueConsumer::class.java)
        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            )
        )
        val pickTaskDao = Mockito.mock(QueuePickTaskDao::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())
        Mockito.`when`(pickTaskDao.pickTask()).thenThrow(IllegalStateException("fail"))
        val listener = Mockito.mock(TaskLifecycleListener::class.java)
        val millisTimeProvider = Mockito.spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))

        val pickedTask = TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTask()

        Assert.assertThat(pickedTask, CoreMatchers.equalTo(null))

        Mockito.verify(millisTimeProvider).millis
        Mockito.verify(queueShard).databaseAccessLayer
        Mockito.verify(pickTaskDao).pickTask()
        Mockito.verifyNoInteractions(listener)
    }
}
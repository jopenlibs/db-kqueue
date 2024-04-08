package io.github.jopenlibs.dbkqueue.internal.runner

import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.stub.TestFixtures
import java.time.Duration

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueRunnerInTransactionTest {
    @Test
    @Throws(Exception::class)
    fun should_wait_notasktimeout_when_no_task_found() = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val queueConsumer = Mockito.mock(QueueConsumer::class.java) as QueueConsumer<Any?>
        val taskPicker = Mockito.mock(TaskPicker::class.java)
        Mockito.`when`(taskPicker.pickTask()).thenReturn(null)
        val taskProcessor = Mockito.mock(TaskProcessor::class.java)
        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())

        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                io.github.jopenlibs.dbkqueue.internal.runner.QueueRunnerInTransactionTest.Companion.testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()
                ).build()
            )
        )
        val status = QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard).runQueue(queueConsumer)

        Assert.assertThat(status, CoreMatchers.equalTo(QueueProcessingStatus.SKIPPED))

        Mockito.verify(queueShard).databaseAccessLayer
        Mockito.verify(taskPicker).pickTask()
        Mockito.verifyNoInteractions(taskProcessor)
    }

    @Test
    @Throws(Exception::class)
    fun should_wait_betweentasktimeout_when_task_found() = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val queueConsumer = Mockito.mock(QueueConsumer::class.java) as QueueConsumer<Any?>
        val taskPicker = Mockito.mock(TaskPicker::class.java)
        val taskRecord = TaskRecord.builder().build()
        Mockito.`when`(taskPicker.pickTask()).thenReturn(taskRecord)
        val taskProcessor = Mockito.mock(TaskProcessor::class.java)
        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.databaseAccessLayer).thenReturn(io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer())


        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                io.github.jopenlibs.dbkqueue.internal.runner.QueueRunnerInTransactionTest.Companion.testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()
                ).build()
            )
        )
        val queueProcessingStatus =
            QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard).runQueue(queueConsumer)

        Assert.assertThat(queueProcessingStatus, CoreMatchers.equalTo(QueueProcessingStatus.PROCESSED))

        Mockito.verify(queueShard).databaseAccessLayer
        Mockito.verify(taskPicker).pickTask()
        Mockito.verify(taskProcessor).processTask(queueConsumer, taskRecord)
    }

    companion object {
        private val testLocation1 = QueueLocation.builder().withTableName("queue_test")
            .withQueueId(QueueId("test_queue1")).build()
    }
}
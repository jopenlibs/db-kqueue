package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.internal.processing.QueueProcessingStatus
import io.github.jopenlibs.dbkqueue.internal.processing.TaskPicker
import io.github.jopenlibs.dbkqueue.internal.processing.TaskProcessor
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueRunnerInSeparateTransactionsTest {
    @Test
    fun should_wait_notasktimeout_when_no_task_found() = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val queueConsumer: QueueConsumer<Any?> = mock()
        val taskPicker: TaskPicker = mock()
        val taskProcessor: TaskProcessor = mock()

        whenever(taskPicker.pickTask()).thenReturn(null)

        whenever(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()
                ).build()
            )
        )
        val status = QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer)

        Assert.assertThat(status, CoreMatchers.equalTo(QueueProcessingStatus.SKIPPED))

        Mockito.verify(taskPicker).pickTask()
        Mockito.verifyNoInteractions(taskProcessor)
    }

    @Test
    fun should_wait_betweentasktimeout_when_task_found() = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val queueConsumer = Mockito.mock(QueueConsumer::class.java) as QueueConsumer<Any?>
        val taskPicker = Mockito.mock(TaskPicker::class.java)
        val taskRecord = TaskRecord.builder().build()
        Mockito.`when`(taskPicker.pickTask()).thenReturn(taskRecord)
        val taskProcessor = Mockito.mock(TaskProcessor::class.java)


        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()
                ).build()
            )
        )
        val status = QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer)

        Assert.assertThat(status, CoreMatchers.equalTo(QueueProcessingStatus.PROCESSED))

        Mockito.verify(taskPicker).pickTask()
        Mockito.verify(taskProcessor).processTask(queueConsumer, taskRecord)
    }

    companion object {
        private val testLocation1 = QueueLocation.builder().withTableName("queue_test")
            .withQueueId(QueueId("test_queue1")).build()
    }
}
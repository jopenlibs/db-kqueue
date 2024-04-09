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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Duration

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueRunnerInSeparateTransactionsTest {
    @Test
    fun should_wait_notasktimeout_when_no_task_found(): Unit = runBlocking {
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

        assertThat(status).isEqualTo(QueueProcessingStatus.SKIPPED)

        verify(taskPicker).pickTask()
        verifyNoInteractions(taskProcessor)
    }

    @Test
    fun should_wait_betweentasktimeout_when_task_found(): Unit = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val queueConsumer: QueueConsumer<Any?> = mock()
        val taskPicker: TaskPicker = mock()
        val taskRecord = TaskRecord.builder().build()
        whenever(taskPicker.pickTask()).thenReturn(taskRecord)
        val taskProcessor: TaskProcessor = mock()


        whenever<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()
                ).build()
            )
        )
        val status = QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer)

        assertThat(status).isEqualTo(QueueProcessingStatus.PROCESSED)

        verify(taskPicker).pickTask()
        verify(taskProcessor).processTask(queueConsumer, taskRecord)
    }

    companion object {
        private val testLocation1 = QueueLocation.builder().withTableName("queue_test")
            .withQueueId(QueueId("test_queue1")).build()
    }
}
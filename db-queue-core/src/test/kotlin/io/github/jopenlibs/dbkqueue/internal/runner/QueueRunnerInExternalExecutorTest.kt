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
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.Executor

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueRunnerInExternalExecutorTest {
    @Test
    fun should_wait_notasktimeout_when_no_task_found(): Unit = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val executor = spy(FakeExecutor())
        val queueConsumer: QueueConsumer<Any?> = mock()
        val taskPicker: TaskPicker = mock()
        val taskProcessor: TaskProcessor = mock()

        whenever(taskPicker.pickTask()).thenReturn(null)

        whenever<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()
                ).build()
            )
        )
        val status = QueueRunnerInExternalExecutor(taskPicker, taskProcessor, executor).runQueue(queueConsumer)

        assertThat(status).isEqualTo(QueueProcessingStatus.SKIPPED)

        verifyNoInteractions(executor)
        verify(taskPicker).pickTask()
        verifyNoInteractions(taskProcessor)
    }

    @Test
    fun should_wait_betweentasktimeout_when_task_found(): Unit = runBlocking {
        val betweenTaskTimeout = Duration.ofHours(1L)
        val noTaskTimeout = Duration.ofMillis(5L)

        val executor = spy(FakeExecutor())
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
        val status = QueueRunnerInExternalExecutor(taskPicker, taskProcessor, executor).runQueue(queueConsumer)

        assertThat(status).isEqualTo(QueueProcessingStatus.PROCESSED)

        verify(executor).execute(any())
        verify(taskPicker).pickTask()
        verify(taskProcessor).processTask(queueConsumer, taskRecord)
    }

    private class FakeExecutor : Executor {
        override fun execute(command: Runnable) {
            command.run()
        }
    }

    companion object {
        private val testLocation1 = QueueLocation.builder().withTableName("queue_test")
            .withQueueId(QueueId("test_queue1")).build()
    }
}
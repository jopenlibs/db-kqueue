package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.ThreadLifecycleListener
import io.github.jopenlibs.dbkqueue.internal.runner.QueueRunner
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.FakeMillisTimeProvider
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueTaskPollerTest {
    @Test
    @Throws(Exception::class)
    fun should_perform_success_lifecycle() = runBlocking {
        val queueLoop: QueueLoop = Mockito.spy(SyncQueueLoop())
        val listener = Mockito.mock(ThreadLifecycleListener::class.java)
        val shardId = QueueShardId("s1")
        val queueConsumer : QueueConsumer<Any?> = mock()
        val location = QueueLocation.builder().withTableName("table")
            .withQueueId(QueueId("queue")).build()
        val waitDuration = Duration.ofMillis(100L)
        whenever(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(waitDuration)
                        .build()
                ).build()
            )
        )
        val queueRunner: QueueRunner = mock()
        whenever(queueRunner.runQueue(queueConsumer)).thenReturn(QueueProcessingStatus.SKIPPED)

        val millisTimeProvider = FakeMillisTimeProvider(mutableListOf(7L, 11L))

        QueueTaskPoller(listener, millisTimeProvider).start(queueLoop, shardId, queueConsumer, queueRunner)

        Mockito.verify(queueLoop).doRun(any())
        Mockito.verify(listener).started(shardId, location)
        Mockito.verify(queueRunner).runQueue(queueConsumer)
        Mockito.verify(listener).executed(shardId, location, false, 4)
        Mockito.verify(queueLoop).doWait(waitDuration, QueueLoop.WaitInterrupt.ALLOW)
        Mockito.verify(listener).finished(shardId, location)
    }


    @Test
    @Throws(Exception::class)
    fun should_perform_crash_lifecycle() = runBlocking {
        val queueLoop: QueueLoop = Mockito.spy(SyncQueueLoop())
        val listener = Mockito.mock(ThreadLifecycleListener::class.java)
        val shardId = QueueShardId("s1")
        val queueConsumer: QueueConsumer<Any?> = mock()
        val location = QueueLocation.builder().withTableName("table")
            .withQueueId(QueueId("queue")).build()
        val fatalCrashTimeout = Duration.ofDays(1L)
        whenever<Any>(queueConsumer.queueConfig).thenReturn(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().withPollSettings(
                    TestFixtures.createPollSettings().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO)
                        .withFatalCrashTimeout(fatalCrashTimeout)
                        .build()
                ).build()
            )
        )
        val queueRunner: QueueRunner = mock()

        val exception = RuntimeException("exc")
        Mockito.`when`(queueRunner.runQueue(queueConsumer)).thenThrow(exception)

        QueueTaskPoller(listener, mock()).start(
            queueLoop,
            shardId,
            queueConsumer,
            queueRunner
        )

        Mockito.verify(queueLoop).doRun(any())
        Mockito.verify(listener).started(shardId, location)
        Mockito.verify(queueRunner).runQueue(queueConsumer)
        Mockito.verify(queueLoop).doWait(fatalCrashTimeout, QueueLoop.WaitInterrupt.DENY)
        Mockito.verify(listener).crashed(shardId, location, exception)
        Mockito.verify(listener).finished(shardId, location)
    }
}
package io.github.jopenlibs.dbkqueue.internal.processing

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop.WaitInterrupt
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import ru.yoomoney.tech.dbqueue.stub.FakeMillisTimeProvider
import ru.yoomoney.tech.dbqueue.stub.TestFixtures
import java.time.Duration

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueTaskPollerTest {
    @Test
    @Throws(Exception::class)
    fun should_perform_success_lifecycle() = runBlocking {
        val queueLoop: QueueLoop = Mockito.spy(io.github.jopenlibs.dbkqueue.internal.processing.SyncQueueLoop())
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
        Mockito.verify(queueLoop).doWait(waitDuration, WaitInterrupt.ALLOW)
        Mockito.verify(listener).finished(shardId, location)
    }


    @Test
    @Throws(Exception::class)
    fun should_perform_crash_lifecycle() = runBlocking {
        val queueLoop: QueueLoop = Mockito.spy(io.github.jopenlibs.dbkqueue.internal.processing.SyncQueueLoop())
        val listener = Mockito.mock(ThreadLifecycleListener::class.java)
        val shardId = QueueShardId("s1")
        val queueConsumer = Mockito.mock(QueueConsumer::class.java) as QueueConsumer<Any?>
        val location = QueueLocation.builder().withTableName("table")
            .withQueueId(QueueId("queue")).build()
        val fatalCrashTimeout = Duration.ofDays(1L)
        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(
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
        val queueRunner = Mockito.mock(QueueRunner::class.java)

        val exception = RuntimeException("exc")
        Mockito.`when`(queueRunner.runQueue(queueConsumer)).thenThrow(exception)

        QueueTaskPoller(listener, Mockito.mock(MillisTimeProvider::class.java)).start(
            queueLoop,
            shardId,
            queueConsumer,
            queueRunner
        )

        Mockito.verify(queueLoop).doRun(any())
        Mockito.verify(listener).started(shardId, location)
        Mockito.verify(queueRunner).runQueue(queueConsumer)
        Mockito.verify(queueLoop).doWait(fatalCrashTimeout, WaitInterrupt.DENY)
        Mockito.verify(listener).crashed(shardId, location, exception)
        Mockito.verify(listener).finished(shardId, location)
    }
}
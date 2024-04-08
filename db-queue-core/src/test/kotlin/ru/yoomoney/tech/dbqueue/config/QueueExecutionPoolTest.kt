package ru.yoomoney.tech.dbqueue.config

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.isA
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop
import ru.yoomoney.tech.dbqueue.internal.processing.QueueTaskPoller
import ru.yoomoney.tech.dbqueue.internal.processing.SyncQueueLoop
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import ru.yoomoney.tech.dbqueue.stub.NoopQueueConsumer
import ru.yoomoney.tech.dbqueue.stub.StringQueueConsumer
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.stub.TestFixtures
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.test.Ignore

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
class QueueExecutionPoolTest {
    @Test
    fun should_start() = runBlocking {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable")
                .withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().withProcessingSettings(
                TestFixtures.createProcessingSettings().withThreadCount(2).build()
            )
                .build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        val executor: ExecutorService = Mockito.spy(DirectExecutor())
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor,
            queueRunner
        ) { queueLoop }
        pool.start()
        Mockito.verify(queueTaskPoller, Mockito.times(2)).start(queueLoop, DEFAULT_SHARD.shardId, consumer, queueRunner)
        Mockito.verify(executor, Mockito.times(2)).submit(ArgumentMatchers.any(Runnable::class.java))
    }

    @Test
    fun should_shutdown() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        Mockito.`when`(executor.submit(ArgumentMatchers.any(Runnable::class.java))).thenReturn(
            Mockito.mock(
                Future::class.java
            )
        )
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { SyncQueueLoop() }
        pool.start()
        pool.shutdown()
        Mockito.verify(executor).shutdownNow()
    }

    @Test
    fun should_pause() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        Mockito.`when`(executor.submit(ArgumentMatchers.any(Runnable::class.java))).thenReturn(
            Mockito.mock(
                Future::class.java
            )
        )
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.start()
        pool.pause()
        Mockito.verify(queueLoop).pause()
    }

    @Test
    fun should_invoke_ispaused() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        Mockito.`when`(executor.submit(ArgumentMatchers.any(Runnable::class.java))).thenReturn(
            Mockito.mock(
                Future::class.java
            )
        )
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.start()
        pool.isPaused
        Mockito.verify(queueLoop).isPaused
    }

    @Test
    fun should_invoke_isterminated() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.isTerminated
        Mockito.verify(executor).isTerminated
    }

    @Test
    fun should_invoke_isshutdown() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.isShutdown
        Mockito.verify(executor).isShutdown
    }

    @Test
    fun should_await_termination() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.awaitTermination(Duration.ofSeconds(10))
        Mockito.verify(executor).awaitTermination(10, TimeUnit.SECONDS)
    }

    @Test
    fun should_wakeup() {
        val queueConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable").withQueueId(QueueId("queue1")).build(),
            TestFixtures.createQueueSettings().build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner = Mockito.mock(QueueRunner::class.java)
        val queueTaskPoller = Mockito.mock(QueueTaskPoller::class.java)
        val executor = Mockito.mock(ExecutorService::class.java)
        val queueLoop = Mockito.mock(QueueLoop::class.java)
        Mockito.`when`(executor.submit(ArgumentMatchers.any(Runnable::class.java))).thenReturn(
            Mockito.mock(
                Future::class.java
            )
        )
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.start()
        pool.wakeup()
        Mockito.verify(queueLoop).doContinue()
    }

    @Test
    fun should_resize_queue_pool() {
        val queueConfig = QueueConfig(
            QueueLocation.builder()
                .withTableName("testTable")
                .withQueueId(QueueId("queue1"))
                .build(),
            TestFixtures.createQueueSettings()
                .withProcessingSettings(
                    TestFixtures.createProcessingSettings().withThreadCount(0).build()
                ).build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner: QueueRunner = mock()
        val queueTaskPoller: QueueTaskPoller = mock()

        val executor = spy(
            ThreadPoolExecutor(
                0,
                Int.MAX_VALUE,
                1L, TimeUnit.MILLISECONDS,
                LinkedBlockingQueue(),
                QueueThreadFactory(
                    queueConfig.location, DEFAULT_SHARD.shardId
                )
            )
        )

        val queueLoop: QueueLoop = mock()
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.start()
        verify(executor, times(0)).submit(isA())
        assertThat(executor.poolSize).isEqualTo(0)
        pool.resizePool(1)
        assertThat(executor.poolSize).isEqualTo(1)
        verify(executor, times(1)).submit(isA())
        pool.resizePool(0)
        Thread.sleep(100)
        assertThat(executor.poolSize).isEqualTo(0)
        verify(executor, times(3)).allowCoreThreadTimeOut(true)
        verify(executor, times(2)).corePoolSize = ArgumentMatchers.eq(0)
        verify(executor, times(3)).purge()
    }

    @Test
    @Disabled("TODO: Unstable")
    fun should_resize_queue_pool_when_settings_changed() {
        // TODO: stabilize the test
        val queueConfig = QueueConfig(
            QueueLocation.builder()
                .withTableName("testTable")
                .withQueueId(QueueId("queue1"))
                .build(),
            TestFixtures.createQueueSettings()
                .withProcessingSettings(
                    TestFixtures.createProcessingSettings().withThreadCount(0).build()
                ).build()
        )
        val consumer: StringQueueConsumer = NoopQueueConsumer(queueConfig)
        val queueRunner: QueueRunner = mock()
        val queueTaskPoller: QueueTaskPoller = mock()
        val executor = spy(
            ThreadPoolExecutor(
                0,
                Int.MAX_VALUE,
                1L, TimeUnit.MILLISECONDS,
                LinkedBlockingQueue(),
                QueueThreadFactory(
                    queueConfig.location, DEFAULT_SHARD.shardId
                )
            )
        )

        val queueLoop: QueueLoop = mock()
        val pool = QueueExecutionPool(
            consumer as QueueConsumer<Any?>, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner
        ) { queueLoop }
        pool.start()
        verify(executor, times(0)).submit(isA())
        assertThat(executor.poolSize).isEqualTo(0)

        queueConfig.settings.processingSettings.setValue(
            TestFixtures.createProcessingSettings().withThreadCount(1).build()
        )
        assertThat(executor.poolSize).isEqualTo(1)
        verify(executor, times(1)).submit(isA())
    }

    companion object {
        private val DEFAULT_SHARD: QueueShard<*> = QueueShard(
            QueueShardId("s1"),
            StubDatabaseAccessLayer()
        )
    }
}
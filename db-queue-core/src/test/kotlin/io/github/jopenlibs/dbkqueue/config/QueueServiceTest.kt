package io.github.jopenlibs.dbkqueue.config

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.settings.ExtSettings
import io.github.jopenlibs.dbkqueue.settings.FailRetryType
import io.github.jopenlibs.dbkqueue.settings.FailureSettings
import io.github.jopenlibs.dbkqueue.settings.PollSettings
import io.github.jopenlibs.dbkqueue.settings.ProcessingMode
import io.github.jopenlibs.dbkqueue.settings.ProcessingSettings
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.settings.QueueSettings
import io.github.jopenlibs.dbkqueue.settings.ReenqueueRetryType
import io.github.jopenlibs.dbkqueue.settings.ReenqueueSettings
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.*
import java.util.function.BiFunction

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
class QueueServiceTest {
    @Test
    @Throws(Exception::class)
    fun should_not_do_any_operations_when_queue_is_not_registered() {
        val consumer: QueueConsumer<Any> = mock()
        val queueId = QueueId("test")

        whenever(consumer.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(queueId).build(),
                TestFixtures.createQueueSettings().build()
            )
        )
        val queueExecutionPool = Mockito.mock(QueueExecutionPool::class.java)
        val queueService = QueueService(
            listOf(DEFAULT_SHARD)
        ) { queueShard: QueueShard<*>?, queueConsumer: QueueConsumer<*>? -> queueExecutionPool }
        val errorMessages: MutableList<String?> = ArrayList()

        queueService.start()
        try {
            queueService.start(queueId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        queueService.pause()
        try {
            queueService.pause(queueId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        queueService.unpause()
        try {
            queueService.unpause(queueId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        queueService.isPaused
        try {
            queueService.isPaused(queueId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        queueService.isShutdown

        try {
            queueService.isShutdown(queueId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        queueService.isTerminated
        try {
            queueService.isTerminated(queueId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        queueService.awaitTermination(Duration.ZERO)
        try {
            queueService.awaitTermination(queueId, Duration.ZERO)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }
        try {
            queueService.wakeup(queueId, DEFAULT_SHARD.shardId)
        } catch (exc: IllegalArgumentException) {
            errorMessages.add(exc.message)
        }

        verifyNoInteractions(queueExecutionPool)
        Assert.assertThat(
            errorMessages.toString(), CoreMatchers.equalTo(
                "[cannot invoke start, queue is not registered: queueId=test, " +
                        "cannot invoke pause, queue is not registered: queueId=test, " +
                        "cannot invoke unpause, queue is not registered: queueId=test, " +
                        "cannot invoke isPaused, queue is not registered: queueId=test, " +
                        "cannot invoke isShutdown, queue is not registered: queueId=test, " +
                        "cannot invoke isTerminated, queue is not registered: queueId=test, " +
                        "cannot invoke awaitTermination, queue is not registered: queueId=test, " +
                        "cannot invoke wakeup, queue is not registered: queueId=test" +
                        "]"
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_not_register_queue_when_already_registered() {
        val consumer: QueueConsumer<Any> = mock()

        whenever(consumer.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build()
            )
        )

        val queueService = QueueService(
            listOf(DEFAULT_SHARD),
            mock(),
            mock()
        )
        Assert.assertTrue(queueService.registerQueue(consumer))
        Assert.assertFalse(queueService.registerQueue(consumer))
    }

    @Test
    fun should_work_with_more_than_one_queue() {
        val consumer1: QueueConsumer<Any> = mock()
        val queueId1 = QueueId("queue1")
        whenever(consumer1.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(queueId1).build(),
                TestFixtures.createQueueSettings().build()
            )
        )
        val consumer2: QueueConsumer<Any> = mock()
        val queueId2 = QueueId("queue2")
        whenever(consumer2.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(queueId2).build(),
                TestFixtures.createQueueSettings().build()
            )
        )
        val queueExecutionPool1: QueueExecutionPool = mock()
        whenever(queueExecutionPool1.isPaused).thenReturn(true)
        whenever(queueExecutionPool1.isShutdown).thenReturn(true)
        whenever(queueExecutionPool1.isTerminated).thenReturn(true)

        val queueExecutionPool2: QueueExecutionPool = mock()
        whenever(queueExecutionPool2.isPaused).thenReturn(true)
        whenever(queueExecutionPool2.isShutdown).thenReturn(true)
        whenever(queueExecutionPool2.isTerminated).thenReturn(true)

        val queueService = QueueService(
            listOf(DEFAULT_SHARD),
            BiFunction<QueueShard<*>, QueueConsumer<Any?>, QueueExecutionPool> { shard: QueueShard<*>, queueConsumer: QueueConsumer<*> ->
                if (queueConsumer.queueConfig.location.queueId.equals(queueId1)) {
                    return@BiFunction queueExecutionPool1
                } else if (queueConsumer.queueConfig.location.queueId.equals(queueId2)) {
                    return@BiFunction queueExecutionPool2
                }
                throw IllegalArgumentException("unknown consumer")
            })
        Assert.assertTrue(queueService.registerQueue(consumer1))
        Assert.assertTrue(queueService.registerQueue(consumer2))
        queueService.start()
        queueService.start(queueId1)
        queueService.pause()
        queueService.pause(queueId1)
        queueService.unpause()
        queueService.unpause(queueId1)
        queueService.isPaused
        queueService.isPaused(queueId1)
        queueService.isShutdown
        queueService.isShutdown(queueId1)
        queueService.isTerminated
        queueService.isTerminated(queueId1)
        queueService.wakeup(queueId1, DEFAULT_SHARD.shardId)

        verify(queueExecutionPool1, times(2)).start()
        verify(queueExecutionPool1, times(2)).pause()
        verify(queueExecutionPool1, times(2)).unpause()
        verify(queueExecutionPool1, times(2)).isPaused
        verify(queueExecutionPool1, times(2)).isShutdown
        verify(queueExecutionPool1, times(2)).isTerminated
        verify(queueExecutionPool1, times(1)).wakeup()

        verify(queueExecutionPool2, times(1)).start()
        verify(queueExecutionPool2, times(1)).pause()
        verify(queueExecutionPool2, times(1)).unpause()
        verify(queueExecutionPool2, times(1)).isPaused
        verify(queueExecutionPool2, times(1)).isShutdown
        verify(queueExecutionPool2, times(1)).isTerminated

        verifyNoMoreInteractions(queueExecutionPool1)
        verifyNoMoreInteractions(queueExecutionPool2)
    }

    @Test
    fun should_work_with_more_than_one_shard() {
        val consumer1: QueueConsumer<Any> = mock()
        val queueId1 = QueueId("queue1")
        whenever(consumer1.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(queueId1).build(),
                TestFixtures.createQueueSettings().build()
            )
        )

        val queueExecutionPool1 = Mockito.mock(QueueExecutionPool::class.java)
        whenever(queueExecutionPool1.isPaused).thenReturn(true)
        whenever(queueExecutionPool1.isShutdown).thenReturn(true)
        whenever(queueExecutionPool1.isTerminated).thenReturn(true)
        val queueExecutionPool2 = Mockito.mock(QueueExecutionPool::class.java)
        whenever(queueExecutionPool2.isPaused).thenReturn(true)
        whenever(queueExecutionPool2.isShutdown).thenReturn(true)
        whenever(queueExecutionPool2.isTerminated).thenReturn(true)

        val shard2: QueueShard<*> = QueueShard(
            QueueShardId("s2"),
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer()
        )

        val queueService = QueueService(
            Arrays.asList(DEFAULT_SHARD, shard2),
            BiFunction<QueueShard<*>, QueueConsumer<Any?>, QueueExecutionPool> { shard: QueueShard<*>, queueConsumer: QueueConsumer<*>? ->
                if (shard.shardId.equals(DEFAULT_SHARD.shardId)) {
                    return@BiFunction queueExecutionPool1
                } else if (shard.shardId.equals(shard2.shardId)) {
                    return@BiFunction queueExecutionPool2
                }
                throw IllegalArgumentException("unknown consumer")
            })
        Assert.assertTrue(queueService.registerQueue(consumer1))
        queueService.start()
        queueService.pause()
        queueService.unpause()
        queueService.isPaused
        queueService.isShutdown
        queueService.isTerminated
        queueService.wakeup(queueId1, DEFAULT_SHARD.shardId)
        queueService.wakeup(queueId1, shard2.shardId)

        verify(queueExecutionPool1, Mockito.times(1)).start()
        verify(queueExecutionPool1, Mockito.times(1)).pause()
        verify(queueExecutionPool1, Mockito.times(1)).unpause()
        verify(queueExecutionPool1, Mockito.times(1)).isPaused
        verify(queueExecutionPool1, Mockito.times(1)).isShutdown
        verify(queueExecutionPool1, Mockito.times(1)).isTerminated
        verify(queueExecutionPool1, Mockito.times(1)).wakeup()

        verify(queueExecutionPool2, Mockito.times(1)).start()
        verify(queueExecutionPool2, Mockito.times(1)).pause()
        verify(queueExecutionPool2, Mockito.times(1)).unpause()
        verify(queueExecutionPool2, Mockito.times(1)).isPaused
        verify(queueExecutionPool2, Mockito.times(1)).isShutdown
        verify(queueExecutionPool2, Mockito.times(1)).isTerminated
        verify(queueExecutionPool2, Mockito.times(1)).wakeup()

        verifyNoMoreInteractions(queueExecutionPool1)
        verifyNoMoreInteractions(queueExecutionPool2)
    }

    @Test
    fun should_await_queue_termination() {
        val consumer = Mockito.mock(QueueConsumer::class.java)
        val queueId = QueueId("queue1")
        whenever<Any>(consumer.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(queueId).build(),
                TestFixtures.createQueueSettings().build()
            )
        )
        val queueExecutionPool = Mockito.mock(QueueExecutionPool::class.java)
        whenever(queueExecutionPool.isTerminated).thenReturn(false)
        whenever(queueExecutionPool.queueShardId).thenReturn(DEFAULT_SHARD.shardId)
        val queueService = QueueService(
            Arrays.asList(DEFAULT_SHARD)
        ) { shard: QueueShard<*>?, queueConsumer: QueueConsumer<*>? -> queueExecutionPool }

        Assert.assertTrue(queueService.registerQueue(consumer))
        Assert.assertThat(
            queueService.awaitTermination(queueId, Duration.ofMinutes(1)),
            CoreMatchers.equalTo(listOf(DEFAULT_SHARD.shardId))
        )
        verify(queueExecutionPool).awaitTermination(Duration.ofMinutes(1))
        verify(queueExecutionPool).isTerminated
    }

    @Test
    fun should_update_queue_configs() {
        val consumer: QueueConsumer<Any> = mock()
        val queueId = QueueId("queue1")
        val oldConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable")
                .withQueueId(queueId).build(),
            QueueSettings.builder()
                .withProcessingSettings(
                    ProcessingSettings.builder()
                        .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
                        .withThreadCount(1).build()
                )
                .withPollSettings(
                    PollSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(0))
                        .withNoTaskTimeout(Duration.ofMillis(0))
                        .withFatalCrashTimeout(Duration.ofSeconds(0)).build()
                )
                .withFailureSettings(
                    FailureSettings.builder()
                        .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                        .withRetryInterval(Duration.ofMinutes(1)).build()
                )
                .withReenqueueSettings(
                    ReenqueueSettings.builder()
                        .withRetryType(ReenqueueRetryType.MANUAL).build()
                )
                .withExtSettings(ExtSettings.builder().withSettings(object : HashMap<String, String>() {
                    init {
                        put("one", "1")
                    }
                }).build())
                .build()
        )
        whenever(consumer.queueConfig).thenReturn(oldConfig)
        val queueExecutionPool = Mockito.mock(QueueExecutionPool::class.java)
        val queueService = QueueService(
            Arrays.asList(DEFAULT_SHARD)
        ) { shard: QueueShard<*>?, queueConsumer: QueueConsumer<*>? -> queueExecutionPool }

        Assert.assertTrue(queueService.registerQueue(consumer))

        val newConfig = QueueConfig(
            QueueLocation.builder().withTableName("testTable")
                .withQueueId(queueId).build(),
            QueueSettings.builder()
                .withProcessingSettings(
                    ProcessingSettings.builder()
                        .withProcessingMode(ProcessingMode.WRAP_IN_TRANSACTION)
                        .withThreadCount(0).build()
                )
                .withPollSettings(
                    PollSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(1))
                        .withNoTaskTimeout(Duration.ofMillis(2))
                        .withFatalCrashTimeout(Duration.ofSeconds(3)).build()
                )
                .withFailureSettings(
                    FailureSettings.builder()
                        .withRetryType(FailRetryType.ARITHMETIC_BACKOFF)
                        .withRetryInterval(Duration.ofMinutes(2)).build()
                )
                .withReenqueueSettings(
                    ReenqueueSettings.builder()
                        .withRetryType(ReenqueueRetryType.FIXED)
                        .withFixedDelay(Duration.ofMinutes(1)).build()
                )
                .withExtSettings(ExtSettings.builder().withSettings(object : HashMap<String, String>() {
                    init {
                        put("two", "2")
                    }
                }).build())
                .build()
        )

        val diff = queueService.updateQueueConfigs(Arrays.asList(newConfig))
        Assert.assertThat(diff.size, CoreMatchers.equalTo(1))
        Assert.assertThat(
            diff[queueId], CoreMatchers.equalTo(
                "" +
                        "processingSettings(threadCount=0<1,processingMode=WRAP_IN_TRANSACTION<SEPARATE_TRANSACTIONS)," +
                        "pollSettings(betweenTaskTimeout=PT0.001S<PT0S,noTaskTimeout=PT0.002S<PT0S,fatalCrashTimeout=PT3S<PT0S)," +
                        "failureSettings(retryType=ARITHMETIC_BACKOFF<GEOMETRIC_BACKOFF,retryInterval=PT2M<PT1M)," +
                        "reenqueueSettings(type=FIXED<MANUAL,fixedDelay=PT1M<null)," +
                        "extSettings(two=2<null,one=null<1)"
            )
        )
        Assert.assertThat(consumer.queueConfig, CoreMatchers.equalTo(newConfig))
    }

    @Test
    fun should_await_termination() {
        val consumer: QueueConsumer<Any> = mock()
        val queueId = QueueId("queue1")
        whenever(consumer.queueConfig).thenReturn(
            QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                    .withQueueId(queueId).build(),
                TestFixtures.createQueueSettings().build()
            )
        )

        val queueExecutionPool: QueueExecutionPool = mock()
        whenever(queueExecutionPool.isTerminated).thenReturn(false)
        whenever(queueExecutionPool.queueShardId).thenReturn(DEFAULT_SHARD.shardId)

        val queueService = QueueService(
            Arrays.asList(DEFAULT_SHARD)
        ) { shard: QueueShard<*>?, queueConsumer: QueueConsumer<*>? -> queueExecutionPool }

        Assert.assertTrue(queueService.registerQueue(consumer))
        Assert.assertThat(
            queueService.awaitTermination(Duration.ofMinutes(1)),
            CoreMatchers.equalTo(listOf(queueId))
        )
        verify(queueExecutionPool).awaitTermination(Duration.ofMinutes(1))
        verify(queueExecutionPool, times(2)).isTerminated
    }

    companion object {
        private val DEFAULT_SHARD: QueueShard<*> = QueueShard(
            QueueShardId("s1"),
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer()
        )
    }
}

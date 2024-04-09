package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.Task
import io.github.jopenlibs.dbkqueue.api.Task.Companion.builder
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.api.TaskPayloadTransformer
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.DatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.FakeMillisTimeProvider
import io.github.jopenlibs.dbkqueue.stub.FakeQueueConsumer
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskProcessorTest {
    @Test
    fun should_succesfully_process_task(): Unit = runBlocking {
        val location = QueueLocation.builder().withTableName("testLocation")
            .withQueueId(QueueId("testQueue")).build()
        val taskRecord =
            TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5))
                .withPayload("testPayload")
                .build()
        val shardId = QueueShardId("s1")
        val transformedPayload = "transformedPayload"
        val queueResult = finish()


        val queueShard: QueueShard<DatabaseAccessLayer> = mock()
        whenever(queueShard.shardId).thenReturn(shardId)
        val listener: TaskLifecycleListener = mock()
        val millisTimeProvider: MillisTimeProvider = spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))
        val resultHandler: TaskResultHandler = mock()
        val transformer: TaskPayloadTransformer<String> = mock()

        whenever(transformer.toObject(taskRecord.payload)).thenReturn(transformedPayload)
        val queueConsumer: FakeQueueConsumer = spy(FakeQueueConsumer(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            ),
            transformer
        ) { r: Task<String>? -> queueResult })

        TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(
            queueConsumer,
            taskRecord
        )

        verify(listener).started(shardId, location, taskRecord)
        verify(millisTimeProvider, times(2)).millis
        verify(queueConsumer).execute(
            builder<String>(shardId)
                .withCreatedAt(taskRecord.createdAt)
                .withPayload(transformedPayload)
                .withAttemptsCount(taskRecord.attemptsCount)
                .withReenqueueAttemptsCount(taskRecord.reenqueueAttemptsCount)
                .withTotalAttemptsCount(taskRecord.totalAttemptsCount)
                .withExtData(emptyMap()).build()
        )
        verify(listener).executed(shardId, location, taskRecord, queueResult, 2)
        verify(resultHandler).handleResult(taskRecord, queueResult)
        verify(listener).finished(shardId, location, taskRecord)
    }

    @Test
    fun should_handle_exception_when_queue_failed(): Unit = runBlocking {
        val location = QueueLocation.builder().withTableName("testLocation")
            .withQueueId(QueueId("testQueue")).build()
        val taskRecord =
            TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5))
                .withPayload("testPayload")
                .build()
        val shardId = QueueShardId("s1")
        val queueException = RuntimeException("fail")


        val queueShard: QueueShard<*> = mock()
        whenever(queueShard.shardId).thenReturn(shardId)
        val listener: TaskLifecycleListener = mock()
        val millisTimeProvider: MillisTimeProvider = mock()
        val resultHandler: TaskResultHandler = mock()
        val transformer: TaskPayloadTransformer<String> = mock()

        whenever(transformer.toObject(taskRecord.payload)).thenReturn(taskRecord.payload)
        val queueConsumer: FakeQueueConsumer = spy(FakeQueueConsumer(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            ),
            transformer
        ) { r: Task<String>? ->
            throw queueException
        })

        TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(
            queueConsumer,
            taskRecord
        )

        verify(listener).started(shardId, location, taskRecord)
        verify(queueConsumer).execute<Any>(any())
        verify(listener).crashed(shardId, location, taskRecord, queueException)
        verify(listener).finished(shardId, location, taskRecord)
    }

    @Test
    fun should_handle_exception_when_result_handler_failed(): Unit = runBlocking {
        val location = QueueLocation.builder().withTableName("testLocation")
            .withQueueId(QueueId("testQueue")).build()
        val taskRecord =
            TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5))
                .withPayload("testPayload")
                .build()
        val shardId = QueueShardId("s1")
        val handlerException = RuntimeException("fail")
        val queueResult = finish()

        val queueShard: QueueShard<*> = mock()
        whenever(queueShard.shardId).thenReturn(shardId)
        val listener: TaskLifecycleListener = mock()
        val millisTimeProvider: MillisTimeProvider = mock()
        val resultHandler: TaskResultHandler = mock()
        doThrow(handlerException).whenever(resultHandler)
            .handleResult(any(), any())

        val transformer: TaskPayloadTransformer<String> = mock()
        whenever(transformer.toObject(taskRecord.payload)).thenReturn(taskRecord.payload)
        val queueConsumer: FakeQueueConsumer = spy(FakeQueueConsumer(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            ),
            transformer
        ) { r: Task<String>? -> queueResult })


        TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(
            queueConsumer,
            taskRecord
        )

        verify(listener).started(shardId, location, taskRecord)
        verify(queueConsumer).execute<Any>(any())
        verify(resultHandler).handleResult(taskRecord, queueResult)
        verify(listener).crashed(shardId, location, taskRecord, handlerException)
        verify(listener).finished(shardId, location, taskRecord)
    }


    private fun ofSeconds(seconds: Int): ZonedDateTime {
        return ZonedDateTime.of(0, 1, 1, 0, 0, seconds, 0, ZoneId.systemDefault())
    }
}
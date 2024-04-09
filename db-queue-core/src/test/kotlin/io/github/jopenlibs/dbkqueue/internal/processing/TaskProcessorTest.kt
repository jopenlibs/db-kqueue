package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
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
import org.junit.Test
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskProcessorTest {
    @Test
    fun should_succesfully_process_task() = runBlocking {
        val location = QueueLocation.builder().withTableName("testLocation")
            .withQueueId(QueueId("testQueue")).build()
        val taskRecord =
            TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5)).withPayload("testPayload")
                .build()
        val shardId = QueueShardId("s1")
        val transformedPayload = "transformedPayload"
        val queueResult = finish()


        val queueShard: QueueShard<DatabaseAccessLayer> = mock()
        Mockito.`when`(queueShard.shardId).thenReturn(shardId)
        val listener = Mockito.mock(TaskLifecycleListener::class.java)
        val millisTimeProvider: MillisTimeProvider = Mockito.spy(FakeMillisTimeProvider(mutableListOf(3L, 5L)))
        val resultHandler = Mockito.mock(TaskResultHandler::class.java)
        val transformer: TaskPayloadTransformer<String> = mock()

        Mockito.`when`(transformer.toObject(taskRecord.payload)).thenReturn(transformedPayload)
        val queueConsumer: QueueConsumer<String?> = spy(FakeQueueConsumer(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            ),
            transformer
        ) { r: Task<String>? -> queueResult }) as QueueConsumer<String?>

        TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(queueConsumer, taskRecord)

        Mockito.verify(listener).started(shardId, location, taskRecord)
        Mockito.verify(millisTimeProvider, Mockito.times(2)).millis
        Mockito.verify(queueConsumer).execute(
            builder<String>(shardId)
                .withCreatedAt(taskRecord.createdAt)
                .withPayload(transformedPayload)
                .withAttemptsCount(taskRecord.attemptsCount)
                .withReenqueueAttemptsCount(taskRecord.reenqueueAttemptsCount)
                .withTotalAttemptsCount(taskRecord.totalAttemptsCount)
                .withExtData(emptyMap()).build()
        )
        Mockito.verify(listener).executed(shardId, location, taskRecord, queueResult, 2)
        Mockito.verify(resultHandler).handleResult(taskRecord, queueResult)
        Mockito.verify(listener).finished(shardId, location, taskRecord)
    }

    @Test
    fun should_handle_exception_when_queue_failed() = runBlocking {
        val location = QueueLocation.builder().withTableName("testLocation")
            .withQueueId(QueueId("testQueue")).build()
        val taskRecord =
            TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5)).withPayload("testPayload")
                .build()
        val shardId = QueueShardId("s1")
        val queueException = RuntimeException("fail")


        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.shardId).thenReturn(shardId)
        val listener = Mockito.mock(TaskLifecycleListener::class.java)
        val millisTimeProvider = Mockito.mock(MillisTimeProvider::class.java)
        val resultHandler = Mockito.mock(TaskResultHandler::class.java)
        val transformer: TaskPayloadTransformer<String> = Mockito.mock(
            TaskPayloadTransformer::class.java
        ) as TaskPayloadTransformer<String>

        Mockito.`when`(transformer.toObject(taskRecord.payload)).thenReturn(taskRecord.payload)
        val queueConsumer: QueueConsumer<String?> = Mockito.spy(FakeQueueConsumer(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            ),
            transformer
        ) { r: Task<String>? ->
            throw queueException
        }) as QueueConsumer<String?>


        TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(queueConsumer, taskRecord)

        Mockito.verify(listener).started(shardId, location, taskRecord)
        Mockito.verify(queueConsumer).execute<Any>(any())
        Mockito.verify(listener).crashed(shardId, location, taskRecord, queueException)
        Mockito.verify(listener).finished(shardId, location, taskRecord)
    }

    @Test
    fun should_handle_exception_when_result_handler_failed() = runBlocking {
        val location = QueueLocation.builder().withTableName("testLocation")
            .withQueueId(QueueId("testQueue")).build()
        val taskRecord =
            TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5)).withPayload("testPayload")
                .build()
        val shardId = QueueShardId("s1")
        val handlerException = RuntimeException("fail")
        val queueResult = finish()

        val queueShard = Mockito.mock(QueueShard::class.java)
        Mockito.`when`(queueShard.shardId).thenReturn(shardId)
        val listener = Mockito.mock(TaskLifecycleListener::class.java)
        val millisTimeProvider = Mockito.mock(MillisTimeProvider::class.java)
        val resultHandler = Mockito.mock(TaskResultHandler::class.java)
        Mockito.doThrow(handlerException).`when`(resultHandler)
            .handleResult(any(), any())
        val transformer: TaskPayloadTransformer<String> = Mockito.mock(
            TaskPayloadTransformer::class.java
        ) as TaskPayloadTransformer<String>
        Mockito.`when`(transformer.toObject(taskRecord.payload)).thenReturn(taskRecord.payload)
        val queueConsumer: QueueConsumer<String?> = Mockito.spy(FakeQueueConsumer(
            QueueConfig(
                location,
                TestFixtures.createQueueSettings().build()
            ),
            transformer
        ) { r: Task<String>? -> queueResult }) as QueueConsumer<String?>


        TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(queueConsumer, taskRecord)

        Mockito.verify(listener).started(shardId, location, taskRecord)
        Mockito.verify(queueConsumer).execute<Any>(any())
        Mockito.verify(resultHandler).handleResult(taskRecord, queueResult)
        Mockito.verify(listener).crashed(shardId, location, taskRecord, handlerException)
        Mockito.verify(listener).finished(shardId, location, taskRecord)
    }


    private fun ofSeconds(seconds: Int): ZonedDateTime {
        return ZonedDateTime.of(0, 1, 1, 0, 0, seconds, 0, ZoneId.systemDefault())
    }
}
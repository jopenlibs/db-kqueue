package io.github.jopenlibs.dbkqueue.config.impl

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.fail
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.reenqueue
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

class LoggingTaskLifecycleListenerTest {
    private lateinit var listAppender: ListAppender<ILoggingEvent>

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        listAppender = ListAppender<ILoggingEvent>()
        listAppender.start()

        val logger = LoggerFactory.getLogger(LoggingTaskLifecycleListener::class.java) as Logger
        logger.addAppender(listAppender)
    }

    @Test
    fun should_log_common_lifecycle() {
        val listener = LoggingTaskLifecycleListener()
        listener.started(SHARD_ID, LOCATION, TASK_RECORD)
        listener.picked(SHARD_ID, LOCATION, TASK_RECORD, 42L)
        listener.finished(SHARD_ID, LOCATION, TASK_RECORD)
        listener.crashed(SHARD_ID, LOCATION, TASK_RECORD, null)

        assertThat(listAppender.list.size).isEqualTo(2)
        assertThat(listAppender.list[0].level).isEqualTo(Level.INFO)
        assertThat(listAppender.list[0].formattedMessage).isEqualTo("consuming task: id=2, attempt=10")
        assertThat(listAppender.list[1].level).isEqualTo(Level.ERROR)
        assertThat(listAppender.list[1].formattedMessage).isEqualTo("error while processing task: task={id=2, attemptsCount=10, reenqueueAttemptsCount=0, totalAttemptsCount=0, createdAt=1970-01-01T00:00Z, nextProcessAt=1970-01-01T00:00:10Z}")
    }

    @Test
    fun should_log_finish_result() {
        val currentTime = Clock.fixed(Instant.EPOCH.plusSeconds(2), ZoneId.of("Z"))
        val listener = LoggingTaskLifecycleListener(currentTime)

        listener.executed(SHARD_ID, LOCATION, TASK_RECORD, finish(), 42L)

        assertThat(listAppender.list.size).isEqualTo(1)
        assertThat(listAppender.list[0].level).isEqualTo(Level.INFO)
        assertThat(listAppender.list[0].formattedMessage).isEqualTo("task finished: id=2, in-queue=PT2S, time=42")
    }

    @Test
    fun should_log_fail_result() {
        val listener = LoggingTaskLifecycleListener(Clock.systemDefaultZone())

        listener.executed(SHARD_ID, LOCATION, TASK_RECORD, fail(), 42L)

        assertThat(listAppender.list.size).isEqualTo(1)
        assertThat(listAppender.list[0].level).isEqualTo(Level.INFO)
        assertThat(listAppender.list[0].formattedMessage).isEqualTo("task failed: id=2, time=42")
    }

    @Test
    fun should_log_reenqueue_result() {
        val listener = LoggingTaskLifecycleListener(Clock.systemDefaultZone())

        listener.executed(SHARD_ID, LOCATION, TASK_RECORD, reenqueue(Duration.ofMinutes(1)), 42L)

        assertThat(listAppender.list.size).isEqualTo(1)
        assertThat(listAppender.list[0].level).isEqualTo(Level.INFO)
        assertThat(listAppender.list[0].formattedMessage).isEqualTo("task reenqueued: id=2, delay=PT1M, time=42")
    }

    companion object {
        val SHARD_ID: QueueShardId = QueueShardId("shardId1")
        val LOCATION: QueueLocation = QueueLocation.builder()
            .withTableName("table1").withQueueId(QueueId("queueId1")).build()
        val TASK_RECORD: TaskRecord = TaskRecord.builder()
            .withId(2L)
            .withCreatedAt(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("Z")))
            .withAttemptsCount(10L)
            .withNextProcessAt(ZonedDateTime.ofInstant(Instant.EPOCH.plusSeconds(10), ZoneId.of("Z")))
            .build()
    }
}
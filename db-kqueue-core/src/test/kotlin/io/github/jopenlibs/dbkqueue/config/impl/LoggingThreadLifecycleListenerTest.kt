package io.github.jopenlibs.dbkqueue.config.impl

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

class LoggingThreadLifecycleListenerTest {
    private lateinit var listAppender: ListAppender<ILoggingEvent>

    @BeforeEach
    fun setUp() {
        listAppender = ListAppender<ILoggingEvent>()
        listAppender.start()

        val logger = LoggerFactory.getLogger(LoggingThreadLifecycleListener::class.java) as Logger
        logger.addAppender(listAppender)
    }

    @Test
    fun should_log_thread_lifecycle() {
        val listener = LoggingThreadLifecycleListener()
        val shardId = QueueShardId("shardId1")
        val location = QueueLocation.builder()
            .withTableName("table1").withQueueId(QueueId("queueId1")).build()
        listener.started(shardId, location)
        listener.executed(shardId, location, true, 42L)
        listener.finished(shardId, location)
        listener.crashed(shardId, location, null)

        assertThat(listAppender.list.size).isEqualTo(1)
        assertThat(listAppender.list[0].level).isEqualTo(Level.ERROR)
        assertThat(listAppender.list[0].formattedMessage).isEqualTo("fatal error in queue thread: shardId=shardId1, location={id=queueId1,table=table1}")
    }
}
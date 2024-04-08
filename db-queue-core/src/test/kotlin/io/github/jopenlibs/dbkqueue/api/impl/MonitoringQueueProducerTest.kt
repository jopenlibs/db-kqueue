package io.github.jopenlibs.dbkqueue.api.impl

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.mockito.stubbing.Answer
import org.slf4j.LoggerFactory
import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import ru.yoomoney.tech.dbqueue.api.EnqueueResult
import ru.yoomoney.tech.dbqueue.api.QueueProducer
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.settings.QueueId
import java.time.Clock

class MonitoringQueueProducerTest {
    private lateinit var listAppender: ListAppender<ILoggingEvent>

    @BeforeEach
    fun setUp() {
        listAppender = ListAppender<ILoggingEvent>()
        listAppender.start()

        val logger = LoggerFactory.getLogger(MonitoringQueueProducer::class.java) as Logger
        logger.addAppender(listAppender)
    }

    @Test
    fun should_invoke_monitoring_callback_and_print_logs() = runBlocking {
        val clock: Clock = mock()

        val expectedResult = EnqueueResult.builder().withShardId(QueueShardId("main")).withEnqueueId(1L).build()
        val producer: QueueProducer<String> = mock()

        val monitoringProducer = MonitoringQueueProducer(
            producer, QueueId("test"),
            { enqueueResult: EnqueueResult?, time: Long ->
                assertThat(enqueueResult).isEqualTo(expectedResult)
                assertThat(time).isEqualTo(4L)
            }, clock
        )

        whenever(producer.enqueue(io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("1"))).doReturn(expectedResult)
        whenever(clock.millis()).thenAnswer(object : Answer<Any> {
            private var count = 0

            override fun answer(invocation: InvocationOnMock): Any {
                count++
                return if (count == 1) {
                    1L
                } else if (count == 2) {
                    5L
                } else {
                    throw IllegalStateException()
                }
            }
        })

        val actualResult = monitoringProducer.enqueue(io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("1"))
        assertThat(actualResult).isEqualTo(expectedResult)

        assertThat(listAppender.list.size).isEqualTo(2)
        assertThat(listAppender.list[0].level).isEqualTo(Level.INFO)
        assertThat(listAppender.list[0].formattedMessage).isEqualTo("enqueuing task: queue=test, delay=PT0S")
        assertThat(listAppender.list[1].level).isEqualTo(Level.INFO)
        assertThat(listAppender.list[1].formattedMessage).isEqualTo("task enqueued: id=1, queueShardId=main")
    }
}
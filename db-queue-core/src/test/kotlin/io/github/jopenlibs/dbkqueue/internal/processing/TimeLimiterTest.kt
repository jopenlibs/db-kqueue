package io.github.jopenlibs.dbkqueue.internal.processing

import org.junit.Assert
import org.junit.Test
import ru.yoomoney.tech.dbqueue.stub.FakeMillisTimeProvider
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Oleg Kandaurov
 * @since 18.10.2019
 */
class TimeLimiterTest {
    @Test
    fun should_not_execute_when_timeout_is_zero() {
        val timeLimiter = TimeLimiter(FakeMillisTimeProvider(emptyList()), Duration.ZERO)
        timeLimiter.execute { ignored: Duration? -> Assert.fail("should not invoke when duration is zero") }
    }

    @Test
    fun should_drain_timeout_to_zero() {
        val timeout = Duration.ofMillis(10)
        val executionCount = AtomicInteger(0)
        val timeLimiter = TimeLimiter(FakeMillisTimeProvider(mutableListOf(0L, 3L, 3L, 11L)), timeout)
        timeLimiter.execute { remainingTimeout: Duration? ->
            executionCount.incrementAndGet()
            Assert.assertEquals(timeout, remainingTimeout)
        }
        timeLimiter.execute { remainingTimeout: Duration? ->
            executionCount.incrementAndGet()
            Assert.assertEquals(Duration.ofMillis(7), remainingTimeout)
        }
        timeLimiter.execute { ignored: Duration? -> Assert.fail("should not invoke when duration is zero") }
        Assert.assertEquals(2, executionCount.get().toLong())
    }
}
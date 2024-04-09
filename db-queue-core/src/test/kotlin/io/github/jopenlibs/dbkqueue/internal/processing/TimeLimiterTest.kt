package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.stub.FakeMillisTimeProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.fail

/**
 * @author Oleg Kandaurov
 * @since 18.10.2019
 */
class TimeLimiterTest {
    @Test
    fun should_not_execute_when_timeout_is_zero() {
        val timeLimiter = TimeLimiter(FakeMillisTimeProvider(emptyList()), Duration.ZERO)
        timeLimiter.execute { _: Duration? -> fail("should not invoke when duration is zero") }
    }

    @Test
    fun should_drain_timeout_to_zero() {
        val timeout = Duration.ofMillis(10)
        val executionCount = AtomicInteger(0)
        val timeLimiter = TimeLimiter(FakeMillisTimeProvider(mutableListOf(0L, 3L, 3L, 11L)), timeout)
        timeLimiter.execute { remainingTimeout: Duration? ->
            executionCount.incrementAndGet()
            assertEquals(timeout, remainingTimeout)
        }
        timeLimiter.execute { remainingTimeout: Duration? ->
            executionCount.incrementAndGet()
            assertEquals(Duration.ofMillis(7), remainingTimeout)
        }
        timeLimiter.execute { _: Duration? -> fail("should not invoke when duration is zero") }
        assertEquals(2, executionCount.get().toLong())
    }
}
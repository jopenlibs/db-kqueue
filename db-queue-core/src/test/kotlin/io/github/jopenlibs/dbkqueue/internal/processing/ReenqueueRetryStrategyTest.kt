package io.github.jopenlibs.dbkqueue.internal.processing

import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.internal.processing.ReenqueueRetryStrategy.Factory.create
import io.github.jopenlibs.dbkqueue.settings.ReenqueueRetryType
import io.github.jopenlibs.dbkqueue.settings.ReenqueueSettings
import org.hamcrest.CoreMatchers
import org.hamcrest.MatcherAssert
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import java.time.Duration
import java.util.*
import java.util.stream.Collectors
import java.util.stream.IntStream

class ReenqueueRetryStrategyTest {
    @JvmField
    @Rule
    var thrown: ExpectedException = ExpectedException.none()

    @Test
    fun should_throw_exception_when_calculate_delay_with_manual_strategy() {
        val settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build()

        val strategy = create(settings)

        thrown.expect(UnsupportedOperationException::class.java)
        thrown.expectMessage("re-enqueue delay must be set explicitly via 'reenqueue(Duration)' method call")

        strategy.calculateDelay(
            createTaskRecord(
                0
            )
        )
    }

    @Test
    fun should_calculate_delay_when_using_fixed_delay_strategy() {
        val fixedDelay = Duration.ofSeconds(10L)
        val settings = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.FIXED)
            .withFixedDelay(fixedDelay)
            .build()

        val strategy = create(settings)

        val delays = IntStream.range(0, 5)
            .mapToObj { reenqueueAttemptsCount: Int ->
                createTaskRecord(
                    reenqueueAttemptsCount.toLong()
                )
            }
            .map { taskRecord: TaskRecord? ->
                strategy.calculateDelay(
                    taskRecord!!
                )
            }
            .collect(Collectors.toList())
        MatcherAssert.assertThat(
            delays,
            CoreMatchers.equalTo(Arrays.asList(fixedDelay, fixedDelay, fixedDelay, fixedDelay, fixedDelay))
        )
    }

    @Test
    fun should_calculate_delay_when_using_sequential_strategy() {
        val settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.SEQUENTIAL)
            .withSequentialPlan(Arrays.asList(Duration.ofSeconds(1L), Duration.ofSeconds(2L), Duration.ofSeconds(3L)))
            .build()

        val strategy = create(settings)

        val delays = IntStream.range(0, 5)
            .mapToObj { reenqueueAttemptsCount: Int ->
                createTaskRecord(
                    reenqueueAttemptsCount.toLong()
                )
            }
            .map { taskRecord: TaskRecord? ->
                strategy.calculateDelay(
                    taskRecord!!
                )
            }
            .collect(Collectors.toList())
        MatcherAssert.assertThat(
            delays, CoreMatchers.equalTo(
                Arrays.asList(
                    Duration.ofSeconds(1L),
                    Duration.ofSeconds(2L),
                    Duration.ofSeconds(3L),
                    Duration.ofSeconds(3L),
                    Duration.ofSeconds(3L)
                )
            )
        )
    }

    @Test
    fun should_calculate_delay_when_using_arithmetic_strategy() {
        val settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC)
            .withInitialDelay(Duration.ofSeconds(10L))
            .withArithmeticStep(Duration.ofSeconds(1L))
            .build()

        val strategy = create(settings)

        val delays = IntStream.range(0, 5)
            .mapToObj { reenqueueAttemptsCount: Int ->
                createTaskRecord(
                    reenqueueAttemptsCount.toLong()
                )
            }
            .map { taskRecord: TaskRecord? ->
                strategy.calculateDelay(
                    taskRecord!!
                )
            }
            .collect(Collectors.toList())
        MatcherAssert.assertThat(
            delays, CoreMatchers.equalTo(
                Arrays.asList(
                    Duration.ofSeconds(10L),
                    Duration.ofSeconds(11L),
                    Duration.ofSeconds(12L),
                    Duration.ofSeconds(13L),
                    Duration.ofSeconds(14L)
                )
            )
        )
    }

    @Test
    fun should_calculate_delay_when_using_geometric_strategy() {
        val settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC)
            .withInitialDelay(Duration.ofSeconds(10L))
            .withGeometricRatio(3L)
            .build()

        val strategy = create(settings)

        val delays = IntStream.range(0, 5)
            .mapToObj { reenqueueAttemptsCount: Int ->
                createTaskRecord(
                    reenqueueAttemptsCount.toLong()
                )
            }
            .map { taskRecord: TaskRecord? ->
                strategy.calculateDelay(
                    taskRecord!!
                )
            }
            .collect(Collectors.toList())
        MatcherAssert.assertThat(
            delays, CoreMatchers.equalTo(
                Arrays.asList(
                    Duration.ofSeconds(10L),
                    Duration.ofSeconds(30L),
                    Duration.ofSeconds(90L),
                    Duration.ofSeconds(270L),
                    Duration.ofSeconds(810L)
                )
            )
        )
    }

    companion object {
        private fun createTaskRecord(reenqueueAttemptsCount: Long): TaskRecord {
            return TaskRecord.builder()
                .withReenqueueAttemptsCount(reenqueueAttemptsCount)
                .withTotalAttemptsCount(reenqueueAttemptsCount).build()
        }
    }
}

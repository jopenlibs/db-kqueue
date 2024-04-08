package ru.yoomoney.tech.dbqueue.internal.processing

import java.time.Duration
import java.util.*
import java.util.function.Consumer

/**
 * Класс, для ограничения времени нескольких действий в заданный таймаут
 *
 * @author Oleg Kandaurov
 * @since 18.10.2019
 */
class TimeLimiter(
    millisTimeProvider: MillisTimeProvider,
    private var remainingTimeout: Duration
) {
    private val millisTimeProvider: MillisTimeProvider = Objects.requireNonNull(millisTimeProvider)
    private var elapsedTime: Duration = Duration.ZERO

    /**
     * Выполнить действие с контролем времени.
     * Если заданный таймаут истёк, то действие не будет выполняться.
     *
     * @param consumer вызываемое действие, с передачей в аргументы оставшегося времени выполнения
     */
    fun execute(consumer: Consumer<Duration>) {
        if (remainingTimeout == Duration.ZERO) {
            return
        }
        val startTime = millisTimeProvider.millis
        consumer.accept(remainingTimeout)
        elapsedTime = elapsedTime.plus(Duration.ofMillis(millisTimeProvider.millis - startTime))
        remainingTimeout = if (remainingTimeout.compareTo(elapsedTime) <= 0) {
            Duration.ZERO
        } else {
            remainingTimeout.minus(elapsedTime)
        }
    }
}

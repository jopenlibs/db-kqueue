package io.github.jopenlibs.dbkqueue.stub

import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class FakeMillisTimeProvider(private val times: List<Long>) : MillisTimeProvider {
    private var invocationCount = 0

    override val millis: Long
        get() {
            val currentTime = times[invocationCount]
            invocationCount++
            return currentTime
        }
}

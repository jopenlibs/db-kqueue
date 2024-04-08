package ru.yoomoney.tech.dbqueue.internal.processing

import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop.WaitInterrupt
import java.time.Duration

class DelegatedSingleQueueLoopExecution internal constructor(private val delegate: QueueLoop) : QueueLoop {
    private var attemptCount = 0

    override suspend fun doRun(runnable: suspend () -> Unit) {
        delegate.doRun {
            if (attemptCount > 0) {
                Thread.currentThread().interrupt()
                return@doRun
            }
            attemptCount++
            runnable()
        }
    }

    override fun doContinue() {
        delegate.doContinue()
    }

    override fun doWait(timeout: Duration?, waitInterrupt: WaitInterrupt) {
        delegate.doWait(timeout, waitInterrupt)
    }

    override fun pause() {
        delegate.pause()
    }

    override fun unpause() {
        delegate.unpause()
    }

    override val isPaused: Boolean
        get() = delegate.isPaused
}

package io.github.jopenlibs.dbkqueue.internal.processing

import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop.WaitInterrupt
import java.time.Duration

class SyncQueueLoop : QueueLoop {
    override suspend fun doRun(runnable: suspend () -> Unit) {
        runnable()
    }

    override fun doContinue() {
    }

    override fun doWait(timeout: Duration?, waitInterrupt: WaitInterrupt) {
    }

    override val isPaused: Boolean
        get() = false

    override fun pause() {
    }

    override fun unpause() {
    }
}

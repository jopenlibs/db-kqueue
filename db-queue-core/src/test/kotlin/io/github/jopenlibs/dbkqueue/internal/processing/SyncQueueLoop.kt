package io.github.jopenlibs.dbkqueue.internal.processing

import java.time.Duration

class SyncQueueLoop : QueueLoop {
    override suspend fun doRun(runnable: suspend () -> Unit) {
        runnable()
    }

    override fun doContinue() {
    }

    override fun doWait(timeout: Duration?, waitInterrupt: QueueLoop.WaitInterrupt) {
    }

    override val isPaused: Boolean
        get() = false

    override fun pause() {
    }

    override fun unpause() {
    }
}

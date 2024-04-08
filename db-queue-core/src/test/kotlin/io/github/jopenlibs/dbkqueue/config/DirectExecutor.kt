package io.github.jopenlibs.dbkqueue.config

import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.TimeUnit

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
internal class DirectExecutor : AbstractExecutorService() {
    override fun execute(command: Runnable) {
        command.run()
    }

    override fun shutdown() {
    }

    override fun shutdownNow(): List<Runnable> {
        return emptyList()
    }

    override fun isShutdown(): Boolean {
        return false
    }

    override fun isTerminated(): Boolean {
        return false
    }

    @Throws(InterruptedException::class)
    override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean {
        return false
    }
}

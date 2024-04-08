package io.github.jopenlibs.dbkqueue.config

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Thread factory for tasks execution pool.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
internal class QueueThreadFactory constructor(
    location: QueueLocation,
    shardId: QueueShardId
) : ThreadFactory {
    private val exceptionHandler: Thread.UncaughtExceptionHandler = QueueUncaughtExceptionHandler()

    private val location: QueueLocation = location

    private val shardId: QueueShardId = shardId

    override fun newThread(runnable: Runnable): Thread {
        val threadName = THREAD_FACTORY_NAME + threadNumber.getAndIncrement()
        val thread: Thread = QueueThread(
            Thread.currentThread().threadGroup, runnable, threadName,
            0, location, shardId
        )
        thread.uncaughtExceptionHandler = exceptionHandler
        return thread
    }

    private class QueueThread(
        group: ThreadGroup?,
        target: Runnable?,
        name: String?,
        stackSize: Long,
        private val location: QueueLocation,
        private val shardId: QueueShardId
    ) : Thread(group, target, name, stackSize) {

        override fun run() {
            log.info("starting queue thread: threadName={}, location={}, shardId={}", name, location, shardId)
            super.run()
            log.info("disposing queue thread: threadName={}, location={}, shardId={}", name, location, shardId)
        }
    }

    private class QueueUncaughtExceptionHandler : Thread.UncaughtExceptionHandler {
        override fun uncaughtException(thread: Thread, throwable: Throwable) {
            log.error("detected uncaught exception", throwable)
        }

        companion object {
            private val log: Logger = LoggerFactory.getLogger(QueueUncaughtExceptionHandler::class.java)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(QueueThreadFactory::class.java)

        private const val THREAD_FACTORY_NAME = "queue-"
        private val threadNumber = AtomicLong(0)
    }
}

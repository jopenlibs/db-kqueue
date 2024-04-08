package ru.yoomoney.tech.dbqueue.config

import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider.SystemMillisTimeProvider
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop.WakeupQueueLoop
import ru.yoomoney.tech.dbqueue.internal.processing.QueueTaskPoller
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings
import ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader
import ru.yoomoney.tech.dbqueue.settings.QueueId
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Supplier

/**
 * Task execution pool: manages start, pause and shutdown of task executors on the assigned shard.
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
internal class QueueExecutionPool(
    private val queueConsumer: QueueConsumer<Any?>,
    private val queueShard: QueueShard<*>,
    private val queueTaskPoller: QueueTaskPoller,
    private val executor: ExecutorService,
    private val queueRunner: QueueRunner,
    private val queueLoopFactory: Supplier<QueueLoop>
) {
    private val queueWorkers: MutableList<QueueWorker> = ArrayList()

    private var started = false

    constructor(
        queueConsumer: QueueConsumer<Any?>,
        queueShard: QueueShard<*>,
        taskLifecycleListener: TaskLifecycleListener,
         threadLifecycleListener: ThreadLifecycleListener
    ) : this(queueConsumer, queueShard,
        QueueTaskPoller(
            threadLifecycleListener,
            SystemMillisTimeProvider()
        ),
        ThreadPoolExecutor(
            queueConsumer.queueConfig.settings.processingSettings.threadCount,
            Int.MAX_VALUE,
            1L, TimeUnit.MILLISECONDS,
            LinkedBlockingQueue<Runnable>(),
            QueueThreadFactory(
                queueConsumer.queueConfig.location, queueShard.shardId
            )
        ),
        QueueRunner.Factory.create(queueConsumer, queueShard, taskLifecycleListener),
        Supplier<QueueLoop> { WakeupQueueLoop() })

    init {
        queueConsumer.queueConfig.settings.processingSettings.registerObserver { oldValue: ProcessingSettings?, newValue: ProcessingSettings ->
            resizePool(
                newValue.threadCount
            )
        }
    }

    private val queueId: QueueId
        get() = queueConsumer.queueConfig.location.queueId

    val queueShardId: QueueShardId
        /**
         * Get identifier of the shard, which will be managed with the execution pool.
         *
         * @return Shard identifier.
         */
        get() = queueShard.shardId

    /**
     * Start task processing in the queue
     */
    fun start() {
        if (!started && !isShutdown) {
            val threadCount = queueConsumer.queueConfig.settings.processingSettings.threadCount
            log.info(
                "starting queue: queueId={}, shardId={}, threadCount={}", queueId, queueShard.shardId,
                threadCount
            )
            for (i in 0 until threadCount) {
                startThread(true)
            }
            setupExecutor(threadCount)
            started = true
        } else {
            log.info("execution pool is already started or underlying executor is closed")
        }
    }

    /**
     * Resize queue execution pool
     *
     * @param newThreadCount thread count for execution pool.
     */
    fun resizePool(newThreadCount: Int) {
        val oldThreadCount = queueWorkers.size
        if (newThreadCount == oldThreadCount) {
            return
        }
        log.info(
            "resizing queue execution pool: queueId={}, shardId={}, oldThreadCount={}, " +
                    "newThreadCount={}",
            queueConsumer.queueConfig.location.queueId,
            queueShard.shardId, oldThreadCount, newThreadCount
        )
        if (newThreadCount > oldThreadCount) {
            for (i in oldThreadCount until newThreadCount) {
                startThread(!isPaused)
            }
        } else {
            for (i in oldThreadCount downTo newThreadCount + 1) {
                disposeThread()
            }
        }
        setupExecutor(newThreadCount)
    }

    private fun setupExecutor(newThreadCount: Int) {
        if (executor is ThreadPoolExecutor) {
            val threadPoolExecutor = executor
            threadPoolExecutor.corePoolSize = newThreadCount
            threadPoolExecutor.allowCoreThreadTimeOut(true)
            threadPoolExecutor.purge()
        }
    }

    private fun startThread(startProcessing: Boolean) {
        val queueLoop = queueLoopFactory.get()
        val future = executor.submit {
            runBlocking {
                queueTaskPoller.start(
                    queueLoop, queueShard.shardId,
                    queueConsumer, queueRunner
                )
            }
        }
        if (startProcessing) {
            queueLoop.unpause()
        }
        queueWorkers.add(QueueWorker(future, queueLoop))
    }

    private fun disposeThread() {
        val queueWorker = queueWorkers[queueWorkers.size - 1]
        queueWorker.future.cancel(true)
        queueWorkers.removeAt(queueWorkers.size - 1)
    }

    /**
     * Stop tasks processing, semantic is the same as for [ExecutorService.shutdownNow]
     */
    fun shutdown() {
        if (started && !isShutdown) {
            log.info("shutting down queue: queueId={}, shardId={}", queueId, queueShard.shardId)
            resizePool(0)
            executor.shutdownNow()
            started = false
        } else {
            log.info("execution pool is already stopped or underlying executor is closed")
        }
    }

    /**
     * Pause task processing.
     * To start the processing again, use [QueueExecutionPool.unpause] method
     */
    fun pause() {
        log.info("pausing queue: queueId={}, shardId={}", queueId, queueShard.shardId)
        queueWorkers.forEach(Consumer<QueueWorker> { queueWorker: QueueWorker -> queueWorker.loop.pause() })
    }

    /**
     * Continue task processing.
     * To pause processing, use [QueueExecutionPool.pause] method
     */
    fun unpause() {
        log.info("unpausing queue: queueId={}, shardId={}", queueId, queueShard.shardId)
        queueWorkers.forEach(Consumer<QueueWorker> { queueWorker: QueueWorker -> queueWorker.loop.unpause() })
    }

    val isPaused: Boolean
        /**
         * Get attribute that the tasks processing was paused with [QueueExecutionPool.pause] method.
         *
         * @return true if the tasks processing was paused.
         */
        get() = queueWorkers.stream().allMatch { it.loop.isPaused }

    val isShutdown: Boolean
        /**
         * Get attribute that the tasks processing was stopped with [QueueExecutionPool.shutdown] method.
         * Semantic is the same as for [ExecutorService.isShutdown].
         *
         * @return true if the tasks processing was stopped.
         */
        get() = executor.isShutdown

    val isTerminated: Boolean
        /**
         * Get attribute that all the processing threads were successfully terminated.
         * Semantic is the same as for [ExecutorService.isTerminated].
         *
         * @return true if all the threads were successfully terminated.
         */
        get() = executor.isTerminated

    /**
     * Wait for tasks (and threads) termination within given timeout.
     * Semantic is the same as for [ExecutorService.awaitTermination].
     *
     * @param timeout waiting timeout
     * @return true if all the threads were successfully terminated within given timeout.
     */
    fun awaitTermination(timeout: Duration): Boolean {
        log.info(
            "awaiting queue termination: queueId={}, shardId={}, timeout={}",
            queueId, queueShard.shardId, timeout
        )
        try {
            return executor.awaitTermination(timeout.seconds, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            return false
        }
    }

    /**
     * Force continue task processing if processing was paused
     * with [QueueConfigsReader.SETTING_NO_TASK_TIMEOUT] event.
     */
    fun wakeup() {
        queueWorkers.forEach(Consumer { queueWorker: QueueWorker -> queueWorker.loop.doContinue() })
    }

    private class QueueWorker(val future: Future<*>, val loop: QueueLoop) {
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(QueueExecutionPool::class.java)
    }
}

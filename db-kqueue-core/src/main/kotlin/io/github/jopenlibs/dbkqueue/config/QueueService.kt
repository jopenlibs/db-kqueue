package io.github.jopenlibs.dbkqueue.config

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.internal.processing.MillisTimeProvider
import io.github.jopenlibs.dbkqueue.internal.processing.TimeLimiter
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.stream.Collectors

/**
 * A service for managing start, pause and shutdown of task processors.
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
class QueueService internal constructor(
    private val queueShards: List<QueueShard<*>>,
    private val queueExecutionPoolFactory: BiFunction<QueueShard<*>, QueueConsumer<Any?>, QueueExecutionPool>
) {
    private val registeredQueues: MutableMap<QueueId, Map<QueueShardId, QueueExecutionPool>> = LinkedHashMap()

    private val registeredConsumer: MutableMap<QueueId, QueueConsumer<*>> = LinkedHashMap()

    constructor(
        queueShards: List<QueueShard<*>>,
        threadLifecycleListener: ThreadLifecycleListener,
        taskLifecycleListener: TaskLifecycleListener
    ) : this(queueShards,
        BiFunction<QueueShard<*>, QueueConsumer<Any?>, QueueExecutionPool> { shard: QueueShard<*>, consumer: QueueConsumer<Any?> ->
            QueueExecutionPool(
                consumer, shard,
                taskLifecycleListener, threadLifecycleListener
            )
        })

    private fun getQueuePools(queueId: QueueId, method: String): Map<QueueShardId, QueueExecutionPool> {
        Objects.requireNonNull(queueId, "queueId")
        Objects.requireNonNull(method, "method")
        require(registeredQueues.containsKey(queueId)) {
            "cannot invoke " + method +
                    ", queue is not registered: queueId=" + queueId
        }
        return registeredQueues[queueId]!!
    }

    /**
     * Register new task processor of given payload type.
     *
     * @param consumer   Task processor.
     * @param <PayloadT> Type of the processor (type of the payload in the task).
     * @return Attribute of successful task processor registration.
    </PayloadT> */
    @Synchronized
    fun <PayloadT> registerQueue(consumer: QueueConsumer<PayloadT>): Boolean {
        val queueId = consumer.queueConfig.location.queueId
        if (registeredQueues.containsKey(queueId)) {
            log.info("queue is already registered: queueId={}", queueId)
            return false
        }
        val queueShardPools: MutableMap<QueueShardId, QueueExecutionPool> = LinkedHashMap()
        queueShards.forEach(Consumer { shard: QueueShard<*> ->
            queueShardPools[shard.shardId] = queueExecutionPoolFactory.apply(shard, consumer as QueueConsumer<Any?>)
        })
        registeredQueues[queueId] = queueShardPools
        registeredConsumer[queueId] = consumer
        return true
    }

    /**
     * Update queue configurations.
     * Applies update to these type of settings:
     *
     *
     * [ProcessingSettings] - supports update only for [ProcessingSettings.threadCount] setting
     *
     *
     * [PollSettings], [FailureSettings], [ReenqueueSettings] - supports update of all settings
     *
     * @param configs new configuration
     * @return settings diff per queue
     * @throws IllegalArgumentException when queue configuration is not found
     */
    @Synchronized
    fun updateQueueConfigs(configs: Collection<QueueConfig>): Map<QueueId, String> {
        val resultDiff: MutableMap<QueueId, String> = LinkedHashMap()
        configs.forEach(Consumer { newConfig: QueueConfig ->
            require(registeredConsumer.containsKey(newConfig.location.queueId)) {
                "cannot update queue configuration" +
                        ", queue is not registered: queueId=" + newConfig.location.queueId
            }
            val queueDiff = StringJoiner(",")
            val actualSettings = registeredConsumer[newConfig.location.queueId]!!.queueConfig!!.settings
            val newSettings = newConfig.settings

            actualSettings.processingSettings.setValue(
                newSettings.processingSettings
            ).ifPresent { newElement: String? -> queueDiff.add(newElement) }
            actualSettings.pollSettings.setValue(
                newSettings.pollSettings
            ).ifPresent { newElement: String? -> queueDiff.add(newElement) }
            actualSettings.failureSettings.setValue(
                newSettings.failureSettings
            ).ifPresent { newElement: String? -> queueDiff.add(newElement) }
            actualSettings.reenqueueSettings.setValue(
                newSettings.reenqueueSettings
            ).ifPresent { newElement: String? -> queueDiff.add(newElement) }
            actualSettings.extSettings.setValue(
                newSettings.extSettings
            ).ifPresent { newElement: String? -> queueDiff.add(newElement) }
            if (!queueDiff.toString().isEmpty()) {
                resultDiff[newConfig.location.queueId] = queueDiff.toString()
            }
        })

        return resultDiff
    }

    /**
     * Start tasks processing in all queues registered in the service.
     */
    @Synchronized
    fun start() {
        log.info("starting all queues")
        registeredQueues.keys.forEach(Consumer(this::start))
    }

    /**
     * Start tasks processing in one given queue.
     *
     * @param queueId Queue identifier.
     */
    @Synchronized
    fun start(queueId: QueueId) {
        Objects.requireNonNull(queueId, "queueId")
        log.info("starting queue: queueId={}", queueId)
        getQueuePools(queueId, "start").values.forEach(Consumer { obj: QueueExecutionPool -> obj.start() })
    }

    /**
     * Stop tasks processing in all queues registered in the service,
     * semantic is the same as for [ExecutorService.shutdownNow].
     */
    @Synchronized
    fun shutdown() {
        log.info("shutting down all queues")
        registeredQueues.keys.forEach(Consumer(this::shutdown))
    }

    /**
     * Stop tasks processing in one given queue,
     * semantic is the same as for [ExecutorService.shutdownNow].
     *
     * @param queueId Queue identifier.
     */
    @Synchronized
    fun shutdown(queueId: QueueId) {
        Objects.requireNonNull(queueId, "queueId")
        log.info("shutting down queue: queueId={}", queueId)
        getQueuePools(queueId, "shutdown").values.forEach(Consumer { obj: QueueExecutionPool -> obj.shutdown() })
    }

    /**
     * Get attribute that the tasks processing was stopped in one specific queue
     * with [QueueService.shutdown] method.
     * Semantic is the same as for [ExecutorService.isShutdown].
     *
     * @param queueId Queue identifier.
     * @return true if the tasks processing was stopped.
     */
    @Synchronized
    fun isShutdown(queueId: QueueId): Boolean {
        Objects.requireNonNull(queueId, "queueId")
        return getQueuePools(queueId, "isShutdown").values.stream()
            .allMatch { obj: QueueExecutionPool -> obj.isShutdown }
    }

    @get:Synchronized
    val isShutdown: Boolean
        /**
         * Get attribute that the tasks processing was stopped in all registered queues
         * with [QueueService.shutdown].
         * Semantic is the same as for [ExecutorService.isShutdown].
         *
         * @return true if the tasks processing was stopped.
         */
        get() = registeredQueues.keys.stream().allMatch { isShutdown(it) }

    /**
     * Get attribute that all the processing task threads were successfully terminated in the specified queue.
     * Semantic is the same as for [ExecutorService.isTerminated].
     *
     * @param queueId Queue identifier.
     * @return true if all the task threads were terminated in specified queue.
     */
    @Synchronized
    fun isTerminated(queueId: QueueId): Boolean {
        return getQueuePools(queueId, "isTerminated").values.stream()
            .allMatch { obj: QueueExecutionPool -> obj.isTerminated }
    }

    @get:Synchronized
    val isTerminated: Boolean
        /**
         * Get attribute that all queues finished their execution and all task threads were terminated.
         * Semantic is the same as for [ExecutorService.isTerminated].
         *
         * @return true if all task threads in all queues were terminated.
         */
        get() = registeredQueues.keys.stream().allMatch { isTerminated(it) }

    /**
     * Pause task processing in specified queue.
     * To start the processing again, use {[QueueService.unpause] method.
     *
     * @param queueId Queue identifier.
     */
    @Synchronized
    fun pause(queueId: QueueId) {
        Objects.requireNonNull(queueId, "queueId")
        log.info("pausing queue: queueId={}", queueId)
        getQueuePools(queueId, "pause").values.forEach(Consumer { obj: QueueExecutionPool -> obj.pause() })
    }

    /**
     * Pause task processing in all queues.
     * To start processing, use [QueueService.unpause] method.
     */
    @Synchronized
    fun pause() {
        log.info("pausing all queues")
        registeredQueues.keys.forEach(Consumer { pause(it) })
    }

    /**
     * Continue task processing in specified queue.
     * To pause processing, use {[QueueService.pause] method.
     *
     * @param queueId Queue identifier.
     */
    @Synchronized
    fun unpause(queueId: QueueId) {
        Objects.requireNonNull(queueId, "queueId")
        log.info("unpausing queue: queueId={}", queueId)
        getQueuePools(queueId, "unpause").values.forEach(Consumer { obj: QueueExecutionPool -> obj.unpause() })
    }

    /**
     * Continue task processing in all queues.
     * To pause processing, use [QueueService.pause] method.
     */
    @Synchronized
    fun unpause() {
        log.info("unpausing all queues")
        registeredQueues.keys.forEach(Consumer(this::unpause))
    }

    @get:Synchronized
    val isPaused: Boolean
        /**
         * Get attribute that all queues were paused with [QueueService.pause] method.
         *
         * @return true if queues were paused.
         */
        get() = registeredQueues.keys.stream().allMatch(this::isPaused)

    /**
     * Get attribute that the specified queue were paused with [QueueService.pause] method.
     *
     * @param queueId Queue identifier.
     * @return true if specified queue were paused.
     */
    @Synchronized
    fun isPaused(queueId: QueueId): Boolean {
        Objects.requireNonNull(queueId, "queueId")
        return getQueuePools(queueId, "isPaused").values.stream()
            .allMatch { obj: QueueExecutionPool -> obj.isPaused }
    }

    /**
     * Wait for tasks (and threads) termination in all queues within given timeout.
     * Semantic is the same as for [ExecutorService.awaitTermination].
     *
     * @param timeout Wait timeout.
     * @return List of queues, which didn't stop their work (didn't terminate).
     */
    @Synchronized
    fun awaitTermination(timeout: Duration): List<QueueId> {
        Objects.requireNonNull(timeout, "timeout")
        log.info("awaiting all queues termination: timeout={}", timeout)
        val timeLimiter = TimeLimiter(MillisTimeProvider.SystemMillisTimeProvider(), timeout)
        registeredQueues.keys.forEach(Consumer { queueId: QueueId ->
            timeLimiter.execute { remainingTimeout: Duration ->
                awaitTermination(
                    queueId,
                    remainingTimeout
                )
            }
        })
        return registeredQueues.keys.stream()
            .filter{queueId -> !isTerminated(queueId)}
            .collect(Collectors.toList())
    }

    /**
     * Wait for tasks (and threads) termination in specified queue within given timeout.
     * Semantic is the same as for [ExecutorService.awaitTermination].
     *
     * @param queueId Queue identifier.
     * @param timeout Wait timeout.
     * @return List of shards, where the work didn't stop (working threads on which were not terminated).
     */
    @Synchronized
    fun awaitTermination(queueId: QueueId, timeout: Duration): List<QueueShardId?> {
        log.info("awaiting queue termination: queueId={}, timeout={}", queueId, timeout)
        val timeLimiter = TimeLimiter(MillisTimeProvider.SystemMillisTimeProvider(), timeout)
        getQueuePools(queueId, "awaitTermination").values
            .forEach(Consumer { queueExecutionPool: QueueExecutionPool ->
                timeLimiter.execute { timeout: Duration ->
                    queueExecutionPool.awaitTermination(
                        timeout
                    )
                }
            })

        return getQueuePools(queueId, "awaitTermination").values.stream()
            .filter { queueExecutionPool: QueueExecutionPool -> !queueExecutionPool.isTerminated }
            .map { obj: QueueExecutionPool -> obj.queueShardId }
            .collect(Collectors.toList())
    }

    /**
     * Force continue task processing in specified queue by given shard identifier.
     *
     *
     * Processing continues only if the queue were paused with
     * [QueueConfigsReader.SETTING_NO_TASK_TIMEOUT] event.
     *
     *
     * It might be useful for queues which interact with the end user,
     * whereas the end users might often expect possibly the quickest response on their actions.
     * Applies right after a task enqueue,
     * therefore should be called only after successful task insertion transaction.
     * Applies also to tests to improve the speed of test execution.
     *
     * @param queueId      Queue identifier.
     * @param queueShardId Shard identifier.
     */
    @Synchronized
    fun wakeup(queueId: QueueId, queueShardId: QueueShardId) {
        val queuePools = getQueuePools(queueId, "wakeup")
        val queueExecutionPool = queuePools[queueShardId]
            ?: throw IllegalArgumentException(
                "cannot wakeup, unknown shard: " +
                        "queueId=" + queueId + ", shardId=" + queueShardId
            )
        queueExecutionPool.wakeup()
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(QueueService::class.java)
    }
}

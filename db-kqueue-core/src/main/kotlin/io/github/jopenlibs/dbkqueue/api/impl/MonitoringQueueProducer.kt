package io.github.jopenlibs.dbkqueue.api.impl

import io.github.jopenlibs.dbkqueue.api.EnqueueResult
import io.github.jopenlibs.dbkqueue.api.QueueProducer
import io.github.jopenlibs.dbkqueue.api.TaskPayloadTransformer
import io.github.jopenlibs.dbkqueue.settings.QueueId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Clock
import java.util.function.BiConsumer

/**
 * Wrapper for queue producer with logging and monitoring support
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 11.06.2021
</PayloadT> */
class MonitoringQueueProducer<PayloadT> internal constructor(
    private val queueProducer: QueueProducer<PayloadT>,
    private val queueId: QueueId,
    private val monitoringCallback: BiConsumer<EnqueueResult?, Long>,
    private val clock: Clock
) : QueueProducer<PayloadT> {
    /**
     * Constructor
     *
     * @param queueProducer      Task producer for the queue
     * @param queueId            Id of the queue
     * @param monitoringCallback Callback invoked after putting a task in the queue.
     * It might help to monitor enqueue time.
     */
    /**
     * Constructor
     *
     * @param queueProducer Task producer for the queue
     * @param queueId       Id of the queue
     */
    @JvmOverloads
    constructor(
        queueProducer: QueueProducer<PayloadT>,
        queueId: QueueId,
        monitoringCallback: BiConsumer<EnqueueResult?, Long> = (BiConsumer { enqueueResult: EnqueueResult?, id: Long? -> })
    ) : this(queueProducer, queueId, monitoringCallback, Clock.systemDefaultZone())

    suspend override fun enqueue(enqueueParams: io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadT>): EnqueueResult {
        log.info("enqueuing task: queue={}, delay={}", queueId, enqueueParams.executionDelay)
        val startTime = clock.millis()
        val enqueueResult = queueProducer.enqueue(enqueueParams)
        log.info("task enqueued: id={}, queueShardId={}", enqueueResult.enqueueId, enqueueResult.shardId)
        val elapsedTime = clock.millis() - startTime
        monitoringCallback.accept(enqueueResult, elapsedTime)
        return enqueueResult
    }

    override val payloadTransformer: TaskPayloadTransformer<PayloadT>
        get() = queueProducer.payloadTransformer

    companion object {
        private val log: Logger = LoggerFactory.getLogger(MonitoringQueueProducer::class.java)
    }
}

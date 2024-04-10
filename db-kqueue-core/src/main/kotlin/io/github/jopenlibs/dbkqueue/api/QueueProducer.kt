package io.github.jopenlibs.dbkqueue.api

/**
 * Task producer for the queue, which adds a new task into the queue.
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 10.07.2017
</PayloadT> */
interface QueueProducer<PayloadT> {
    /**
     * Add a new task into the queue
     *
     * @param enqueueParams Parameters with typed payload to enqueue the task
     * @return Enqueue result
     */
    suspend fun enqueue(enqueueParams: io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadT>): EnqueueResult

    val payloadTransformer: TaskPayloadTransformer<PayloadT>
}

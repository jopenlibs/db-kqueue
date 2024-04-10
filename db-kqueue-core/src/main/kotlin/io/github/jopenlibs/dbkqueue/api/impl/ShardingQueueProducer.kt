package io.github.jopenlibs.dbkqueue.api.impl

import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.api.EnqueueResult
import io.github.jopenlibs.dbkqueue.api.QueueProducer
import io.github.jopenlibs.dbkqueue.api.QueueShardRouter
import io.github.jopenlibs.dbkqueue.api.TaskPayloadTransformer
import io.github.jopenlibs.dbkqueue.config.DatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.settings.QueueConfig

/**
 * Wrapper for queue producer wrapper with sharding support.
 *
 * @param <PayloadTaskT>         The type of the payload in the task
 * @param <DatabaseAccessLayerT> The type of the database access layer
 * @author Oleg Kandaurov
 * @since 11.06.2021
</DatabaseAccessLayerT></PayloadTaskT> */
class ShardingQueueProducer<PayloadTaskT, DatabaseAccessLayerT : DatabaseAccessLayer>
    (
    private val queueConfig: QueueConfig,
    override val payloadTransformer: TaskPayloadTransformer<PayloadTaskT>,
    private val queueShardRouter: QueueShardRouter<PayloadTaskT, DatabaseAccessLayerT>
) : QueueProducer<PayloadTaskT> {

    override suspend fun enqueue(enqueueParams: EnqueueParams<PayloadTaskT>): EnqueueResult {
        val queueShard = queueShardRouter.resolveShard(enqueueParams)
        val rawEnqueueParams = EnqueueParams<String?>()
            .withPayload(payloadTransformer.fromObject(enqueueParams.payload))
            .withExecutionDelay(enqueueParams.executionDelay)
            .withExtData(enqueueParams.getExtData())

        val enqueueId = queueShard.databaseAccessLayer.transact {
            queueShard.databaseAccessLayer.queueDao.enqueue(
                queueConfig.location,
                rawEnqueueParams
            )
        }
        return EnqueueResult.builder()
            .withShardId(queueShard.shardId)
            .withEnqueueId(enqueueId)
            .build()
    }
}

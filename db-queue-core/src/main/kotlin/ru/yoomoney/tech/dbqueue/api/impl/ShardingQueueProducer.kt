package ru.yoomoney.tech.dbqueue.api.impl

import ru.yoomoney.tech.dbqueue.api.EnqueueParams
import ru.yoomoney.tech.dbqueue.api.EnqueueResult
import ru.yoomoney.tech.dbqueue.api.QueueProducer
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.settings.QueueConfig

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

package io.github.jopenlibs.dbkqueue.api.impl

import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.api.QueueShardRouter
import io.github.jopenlibs.dbkqueue.config.DatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.config.QueueShard

/**
 * Shard router without sharding. Might be helpful if you have single database instance.
 *
 * @param <PayloadT>             The type of the payload in the task
 * @param <DatabaseAccessLayerT> The type of the database access layer
 * @author Oleg Kandaurov
 * @since 11.06.2021
</DatabaseAccessLayerT></PayloadT> */
class SingleQueueShardRouter<PayloadT, DatabaseAccessLayerT : DatabaseAccessLayer>
    (val queueShard: QueueShard<DatabaseAccessLayerT>) : QueueShardRouter<PayloadT, DatabaseAccessLayerT> {

    override fun resolveShard(enqueueParams: EnqueueParams<PayloadT>): QueueShard<DatabaseAccessLayerT> {
        return queueShard
    }
}

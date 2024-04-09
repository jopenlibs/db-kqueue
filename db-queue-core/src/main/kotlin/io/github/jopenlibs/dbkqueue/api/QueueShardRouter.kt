package io.github.jopenlibs.dbkqueue.api

import io.github.jopenlibs.dbkqueue.config.DatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.config.QueueShard

/**
 * Dispatcher for sharding support.
 *
 *
 * It evaluates designated shard based on task parameters.
 *
 * @param <PayloadT>             The type of the payload in the task
 * @param <DatabaseAccessLayerT> The type of the database access layer
 * @author Oleg Kandaurov
 * @since 11.06.2021
</DatabaseAccessLayerT></PayloadT> */
interface QueueShardRouter<PayloadT, DatabaseAccessLayerT : DatabaseAccessLayer> {
    /**
     * Get designated shard for task parameters
     *
     * @param enqueueParams Parameters with typed payload to enqueue the task
     * @return Shard where task will be processed on
     */
    fun resolveShard(enqueueParams: EnqueueParams<PayloadT>): QueueShard<DatabaseAccessLayerT>
}

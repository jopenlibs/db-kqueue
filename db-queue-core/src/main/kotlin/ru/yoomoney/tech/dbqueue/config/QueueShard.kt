package ru.yoomoney.tech.dbqueue.config

/**
 * Properties for connection to a database shard.
 *
 * @param <DatabaseAccessLayerT> type of database access layer.
 * @author Oleg Kandaurov
 * @since 13.08.2018
</DatabaseAccessLayerT> */
class QueueShard<DatabaseAccessLayerT : DatabaseAccessLayer>(
    /**
     * Get shard identifier.
     *
     * @return Shard identifier.
     */
    val shardId: QueueShardId,

    /**
     * Get database access layer.
     *
     * @return database access layer.
     */
    val databaseAccessLayer: DatabaseAccessLayerT
)
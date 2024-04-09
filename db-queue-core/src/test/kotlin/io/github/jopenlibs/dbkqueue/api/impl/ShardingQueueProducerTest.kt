package io.github.jopenlibs.dbkqueue.api.impl

import io.github.jopenlibs.dbkqueue.api.EnqueueResult
import io.github.jopenlibs.dbkqueue.api.QueueShardRouter
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever

class ShardingQueueProducerTest {
    @Test
    fun should_insert_task_on_designated_shard() {
        runBlocking {
            val stubDatabaseAccessLayer = StubDatabaseAccessLayer()
            val firstShard = QueueShard(
                QueueShardId("first"),
                stubDatabaseAccessLayer
            )
            val secondShard = QueueShard(
                QueueShardId("second"),
                stubDatabaseAccessLayer
            )

            val queueConfig = QueueConfig(
                QueueLocation.builder()
                    .withTableName("testTable")
                    .withQueueId(QueueId("main")).build(),
                TestFixtures.createQueueSettings().build()
            )

            val queueDao = stubDatabaseAccessLayer.queueDao
            whenever(
                queueDao.enqueue(queueConfig.location, io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("1"))
            ).doReturn(11L)

            whenever(
                queueDao.enqueue(
                    queueConfig.location,
                    io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("2")
                )
            ).doReturn(22L)

            val queueProducer: ShardingQueueProducer<String, StubDatabaseAccessLayer> =
                ShardingQueueProducer(
                    queueConfig, NoopPayloadTransformer.instance, StubQueueShardRouter(firstShard, secondShard)
                )

            val enqueueResult1 = queueProducer.enqueue(io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("1"))
            assertThat(enqueueResult1).isEqualTo(
                EnqueueResult.builder().withEnqueueId(11L)
                    .withShardId(firstShard.shardId).build()
            )

            val enqueueResult2 = queueProducer.enqueue(io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("2"))
            assertThat(enqueueResult2).isEqualTo(
                EnqueueResult.builder().withEnqueueId(22L)
                    .withShardId(secondShard.shardId).build()
            )
        }
    }

    private class StubQueueShardRouter(
        private val firstShard: QueueShard<StubDatabaseAccessLayer>,
        private val secondShard: QueueShard<StubDatabaseAccessLayer>
    ) : QueueShardRouter<String, StubDatabaseAccessLayer> {
        override fun resolveShard(enqueueParams: io.github.jopenlibs.dbkqueue.api.EnqueueParams<String>): QueueShard<StubDatabaseAccessLayer> {
            if (enqueueParams.payload == "1") {
                return firstShard
            } else if (enqueueParams.payload == "2") {
                return secondShard
            }
            throw IllegalStateException()
        }
    }
}
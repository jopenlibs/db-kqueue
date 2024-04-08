package ru.yoomoney.tech.dbqueue.api.impl

import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import ru.yoomoney.tech.dbqueue.api.EnqueueParams
import ru.yoomoney.tech.dbqueue.api.EnqueueResult
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.stub.TestFixtures

class ShardingQueueProducerTest {
    @Test
    fun should_insert_task_on_designated_shard() = runBlocking {
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
            queueDao.enqueue(queueConfig.location, EnqueueParams.create("1"))
        ).doReturn(11L)

        whenever(
            queueDao.enqueue(
                queueConfig.location,
                EnqueueParams.create("2")
            )
        ).doReturn(22L)

        val queueProducer: ShardingQueueProducer<String, StubDatabaseAccessLayer> =
            ShardingQueueProducer(
                queueConfig, NoopPayloadTransformer.instance, StubQueueShardRouter(firstShard, secondShard)
            )

        val enqueueResult1 = queueProducer.enqueue(EnqueueParams.create("1"))
        Assert.assertThat(
            enqueueResult1, CoreMatchers.equalTo(
                EnqueueResult.builder().withEnqueueId(11L)
                    .withShardId(firstShard.shardId).build()
            )
        )

        val enqueueResult2 = queueProducer.enqueue(EnqueueParams.create("2"))
        Assert.assertThat(
            enqueueResult2, CoreMatchers.equalTo(
                EnqueueResult.builder().withEnqueueId(22L)
                    .withShardId(secondShard.shardId).build()
            )
        )
    }

    private class StubQueueShardRouter(
        private val firstShard: QueueShard<StubDatabaseAccessLayer>,
        private val secondShard: QueueShard<StubDatabaseAccessLayer>
    ) : QueueShardRouter<String, StubDatabaseAccessLayer> {
        override fun resolveShard(enqueueParams: EnqueueParams<String>): QueueShard<StubDatabaseAccessLayer> {
            if (enqueueParams.payload == "1") {
                return firstShard
            } else if (enqueueParams.payload == "2") {
                return secondShard
            }
            throw IllegalStateException()
        }
    }
}
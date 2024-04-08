package ru.yoomoney.tech.dbqueue.api.impl

import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import ru.yoomoney.tech.dbqueue.api.EnqueueParams
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer

class SingleQueueShardRouterTest {
    @Test
    fun should_return_single_shard() {
        val main = QueueShard(
            QueueShardId("main"),
            StubDatabaseAccessLayer()
        )
        val router = SingleQueueShardRouter<String, StubDatabaseAccessLayer>(main)
        Assert.assertThat(router.resolveShard(EnqueueParams.create("1")), CoreMatchers.equalTo(main))
    }
}
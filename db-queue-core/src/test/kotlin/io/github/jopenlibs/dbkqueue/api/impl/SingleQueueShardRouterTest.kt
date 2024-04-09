package io.github.jopenlibs.dbkqueue.api.impl

import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test

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
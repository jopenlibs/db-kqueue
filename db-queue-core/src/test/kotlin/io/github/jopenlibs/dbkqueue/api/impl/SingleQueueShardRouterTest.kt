package io.github.jopenlibs.dbkqueue.api.impl

import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.CoreMatchers
import org.junit.Test

class SingleQueueShardRouterTest {
    @Test
    fun should_return_single_shard() {
        val main = QueueShard(
            QueueShardId("main"),
            StubDatabaseAccessLayer()
        )
        val router = SingleQueueShardRouter<String, StubDatabaseAccessLayer>(main)
        assertThat(router.resolveShard(EnqueueParams.create("1"))).isEqualTo(CoreMatchers.equalTo(main))
    }
}
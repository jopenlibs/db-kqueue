package io.github.jopenlibs.dbkqueue.api.impl

import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import io.github.jopenlibs.dbkqueue.api.EnqueueParams
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer

class SingleQueueShardRouterTest {
    @Test
    fun should_return_single_shard() {
        val main = QueueShard(
            QueueShardId("main"),
            io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer()
        )
        val router = SingleQueueShardRouter<String, io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer>(main)
        Assert.assertThat(router.resolveShard(io.github.jopenlibs.dbkqueue.api.EnqueueParams.create("1")), CoreMatchers.equalTo(main))
    }
}
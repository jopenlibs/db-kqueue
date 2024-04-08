package io.github.jopenlibs.dbkqueue.config.impl

import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener
import ru.yoomoney.tech.dbqueue.settings.QueueLocation

/**
 * Empty listener for task processing thread in the queue.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
class NoopThreadLifecycleListener : ThreadLifecycleListener {
    override fun started(shardId: QueueShardId, location: QueueLocation) {
    }

    override fun executed(
        shardId: QueueShardId?, location: QueueLocation?,
        taskProcessed: Boolean, threadBusyTime: Long
    ) {
    }

    override fun finished(shardId: QueueShardId, location: QueueLocation) {
    }

    override fun crashed(
        shardId: QueueShardId, location: QueueLocation,
        exc: Throwable?
    ) {
    }

    companion object {
        val instance: NoopThreadLifecycleListener = NoopThreadLifecycleListener()
    }
}

package io.github.jopenlibs.dbkqueue.config.impl

import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.ThreadLifecycleListener
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Thread listener with logging support
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
class LoggingThreadLifecycleListener : ThreadLifecycleListener {
    override fun started(shardId: QueueShardId, location: QueueLocation) {
    }

    override fun executed(
        shardId: QueueShardId?,
        location: QueueLocation?,
        taskProcessed: Boolean,
        threadBusyTime: Long
    ) {
    }

    override fun finished(shardId: QueueShardId, location: QueueLocation) {
    }

    override fun crashed(
        shardId: QueueShardId, location: QueueLocation,
        exc: Throwable?
    ) {
        log.error(
            "fatal error in queue thread: shardId={}, location={}", shardId.asString(),
            location, exc
        )
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(LoggingThreadLifecycleListener::class.java)
    }
}

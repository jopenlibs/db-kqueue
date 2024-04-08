package io.github.jopenlibs.dbkqueue.config.impl

import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import java.util.*
import java.util.function.Consumer

/**
 * Composite listener. It allows combining several listeners into one.
 *
 * Listeners for started events is executed in straight order.
 * Listeners for executed, finished and crashed events are executed in reverse order.
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
class CompositeThreadLifecycleListener(private val listeners: List<ThreadLifecycleListener>) : ThreadLifecycleListener {
    private val reverseListeners: List<ThreadLifecycleListener?> = ArrayList(listeners)

    /**
     * Constructor
     *
     * @param listeners thread listeners
     */
    init {
        Collections.reverse(reverseListeners)
    }

    override fun started(shardId: QueueShardId, location: QueueLocation) {
        listeners.forEach(Consumer { l: ThreadLifecycleListener? -> l!!.started(shardId, location) })
    }

    override fun executed(
        shardId: QueueShardId?,
        location: QueueLocation?,
        taskProcessed: Boolean,
        threadBusyTime: Long
    ) {
        reverseListeners.forEach(Consumer { l: ThreadLifecycleListener? ->
            l!!.executed(
                shardId,
                location,
                taskProcessed,
                threadBusyTime
            )
        })
    }

    override fun finished(shardId: QueueShardId, location: QueueLocation) {
        reverseListeners.forEach(Consumer { l: ThreadLifecycleListener? -> l!!.finished(shardId, location) })
    }

    override fun crashed(shardId: QueueShardId, location: QueueLocation, exc: Throwable?) {
        reverseListeners.forEach(Consumer { l: ThreadLifecycleListener? -> l!!.crashed(shardId, location, exc) })
    }
}

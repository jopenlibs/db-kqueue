package io.github.jopenlibs.dbkqueue.config.impl

import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.ThreadLifecycleListener
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.*

class CompositeThreadLifecycleListenerTest {
    @Test
    fun should_handle_started_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener = StubThreadLifecycleListener("1", events)
        val secondListener = StubThreadLifecycleListener("2", events)
        val compositeListener = CompositeThreadLifecycleListener(
            listOf(firstListener, secondListener)
        )
        compositeListener.started(SHARD_ID, LOCATION)
        assertThat<List<String>>(events).isEqualTo(mutableListOf("1:started", "2:started"))
    }

    @Test
    fun should_handle_executed_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener = StubThreadLifecycleListener("1", events)
        val secondListener = StubThreadLifecycleListener("2", events)
        val compositeListener = CompositeThreadLifecycleListener(
            listOf(firstListener, secondListener)
        )
        compositeListener.executed(SHARD_ID, LOCATION, true, 42L)
        assertThat<List<String>>(events).isEqualTo(mutableListOf("2:executed", "1:executed"))
    }

    @Test
    fun should_handle_finished_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener = StubThreadLifecycleListener("1", events)
        val secondListener = StubThreadLifecycleListener("2", events)
        val compositeListener = CompositeThreadLifecycleListener(
            listOf(firstListener, secondListener)
        )
        compositeListener.finished(SHARD_ID, LOCATION)

        assertThat<List<String>>(events).isEqualTo(mutableListOf("2:finished", "1:finished"))
    }

    @Test
    fun should_handle_crashed_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener = StubThreadLifecycleListener("1", events)
        val secondListener = StubThreadLifecycleListener("2", events)
        val compositeListener = CompositeThreadLifecycleListener(
            listOf(firstListener, secondListener)
        )
        compositeListener.crashed(SHARD_ID, LOCATION, null)

        assertThat<List<String>>(events).isEqualTo(mutableListOf("2:crashed", "1:crashed"))
    }

    class StubThreadLifecycleListener(private val id: String, private val  events: MutableList<String>) :
        ThreadLifecycleListener {
        override fun started(shardId: QueueShardId, location: QueueLocation) {
            events.add("$id:started")
        }

        override fun executed(
            shardId: QueueShardId?,
            location: QueueLocation?,
            taskProcessed: Boolean,
            threadBusyTime: Long
        ) {
            events.add("$id:executed")
        }

        override fun finished(shardId: QueueShardId, location: QueueLocation) {
            events.add("$id:finished")
        }

        override fun crashed(shardId: QueueShardId, location: QueueLocation, exc: Throwable?) {
            events.add("$id:crashed")
        }
    }

    companion object {
        val SHARD_ID: QueueShardId = QueueShardId("shardId1")
        val LOCATION: QueueLocation = QueueLocation.builder()
            .withTableName("table1").withQueueId(QueueId("queueId1")).build()
    }
}
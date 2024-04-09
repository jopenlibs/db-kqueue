package io.github.jopenlibs.dbkqueue.config.impl

import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.api.TaskRecord
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import java.util.*

class CompositeTaskLifecycleListenerTest {
    @Test
    fun should_handle_picked_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.picked(
            SHARD_ID,
            LOCATION,
            TASK_RECORD, 42L)
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("1:picked", "2:picked")))
    }

    @Test
    fun should_handle_started_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.started(
            SHARD_ID,
            LOCATION,
            TASK_RECORD
        )
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("1:started", "2:started")))
    }

    @Test
    fun should_handle_executed_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.executed(
            SHARD_ID,
            LOCATION,
            TASK_RECORD,
            finish(),
            42L
        )
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("2:executed", "1:executed")))
    }

    @Test
    fun should_handle_finished_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.finished(
            SHARD_ID,
            LOCATION,
            TASK_RECORD
        )
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("2:finished", "1:finished")))
    }

    @Test
    fun should_handle_crashed_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.crashed(
            SHARD_ID,
            LOCATION,
            TASK_RECORD, null)
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("2:crashed", "1:crashed")))
    }

    class StubTaskLifecycleListener(
        private val id: String,
        private val events: MutableList<String>
    ) : TaskLifecycleListener {

        override fun picked(
            shardId: QueueShardId,
            location: QueueLocation,
            taskRecord: TaskRecord,
            pickTaskTime: Long
        ) {
            events.add("$id:picked")
        }

        override fun started(shardId: QueueShardId, location: QueueLocation, taskRecord: TaskRecord) {
            events.add("$id:started")
        }

        override fun executed(
            shardId: QueueShardId,
            location: QueueLocation,
            taskRecord: TaskRecord,
            executionResult: TaskExecutionResult,
            processTaskTime: Long
        ) {
            events.add("$id:executed")
        }

        override fun finished(shardId: QueueShardId, location: QueueLocation, taskRecord: TaskRecord) {
            events.add("$id:finished")
        }

        override fun crashed(shardId: QueueShardId, location: QueueLocation, taskRecord: TaskRecord, exc: Exception?) {
            events.add("$id:crashed")
        }
    }

    companion object {
        val SHARD_ID: QueueShardId = QueueShardId("shardId1")
        val LOCATION: QueueLocation = QueueLocation.builder()
            .withTableName("table1").withQueueId(QueueId("queueId1")).build()
        val TASK_RECORD: TaskRecord = TaskRecord.builder().build()
    }
}
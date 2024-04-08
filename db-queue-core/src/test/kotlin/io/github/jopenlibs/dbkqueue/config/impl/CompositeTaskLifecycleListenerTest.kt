package io.github.jopenlibs.dbkqueue.config.impl

import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult.Companion.finish
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import java.util.*

class CompositeTaskLifecycleListenerTest {
    @Test
    fun should_handle_picked_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.picked(
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.SHARD_ID,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.LOCATION,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.TASK_RECORD, 42L)
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("1:picked", "2:picked")))
    }

    @Test
    fun should_handle_started_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.started(
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.SHARD_ID,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.LOCATION,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.TASK_RECORD
        )
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("1:started", "2:started")))
    }

    @Test
    fun should_handle_executed_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.executed(
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.SHARD_ID,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.LOCATION,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.TASK_RECORD, finish(), 42L)
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("2:executed", "1:executed")))
    }

    @Test
    fun should_handle_finished_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.finished(
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.SHARD_ID,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.LOCATION,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.TASK_RECORD
        )
        Assert.assertThat<List<String>>(events, CoreMatchers.equalTo(mutableListOf("2:finished", "1:finished")))
    }

    @Test
    fun should_handle_crashed_in_order() {
        val events: MutableList<String> = ArrayList()
        val firstListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "1",
                events
            )
        val secondListener =
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.StubTaskLifecycleListener(
                "2",
                events
            )
        val compositeListener = CompositeTaskLifecycleListener(
            Arrays.asList(firstListener, secondListener)
        )
        compositeListener.crashed(
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.SHARD_ID,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.LOCATION,
            io.github.jopenlibs.dbkqueue.config.impl.CompositeTaskLifecycleListenerTest.Companion.TASK_RECORD, null)
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
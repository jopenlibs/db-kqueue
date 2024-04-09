package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.api.Task
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.config.TaskLifecycleListener
import io.github.jopenlibs.dbkqueue.internal.runner.QueueRunner.Factory.create
import io.github.jopenlibs.dbkqueue.settings.ProcessingMode
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.StringQueueConsumer
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import java.util.*
import java.util.concurrent.Executor

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class QueueRunnerSpringQueuePickTaskDaoQueueDaoFactoryTest {
    private class ConsumerWithExternalExecutor(
        override val queueConfig: QueueConfig,
        val executor: Executor?
    ) : StringQueueConsumer(queueConfig) {
        override fun <T> execute(task: Task<T>): TaskExecutionResult {
            return finish()
        }

        override fun executor(): Optional<Executor> {
            return Optional.ofNullable(executor)
        }
    }

    @Test
    @Throws(Exception::class)
    fun should_return_external_executor_runner() {
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueConsumer: QueueConsumer<*> = ConsumerWithExternalExecutor(
            QueueConfig(location, settings), Mockito.mock(
                Executor::class.java
            )
        )
        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            Mockito.mock(TaskLifecycleListener::class.java)
        )

        Assert.assertThat(
            queueRunner, CoreMatchers.instanceOf(
                QueueRunnerInExternalExecutor::class.java
            )
        )
    }

    @Test(expected = IllegalArgumentException::class)
    @Throws(Exception::class)
    fun should_throw_exception_when_no_external_executor_runner() {
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueConsumer: QueueConsumer<*> = object : StringQueueConsumer(QueueConfig(location, settings)) {
            override fun <T> execute(task: Task<T>): TaskExecutionResult {
                return finish()
            }
        }
        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            Mockito.mock(TaskLifecycleListener::class.java)
        )

        Assert.assertThat(
            queueRunner, CoreMatchers.instanceOf(
                QueueRunnerInExternalExecutor::class.java
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_return_separate_transactions_runner() {
        val queueConsumer = Mockito.mock(QueueConsumer::class.java)
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(QueueConfig(location, settings))

        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            Mockito.mock(TaskLifecycleListener::class.java)
        )

        Assert.assertThat(
            queueRunner, CoreMatchers.instanceOf(
                QueueRunnerInSeparateTransactions::class.java
            )
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_return_wrap_in_transaction_runner() {
        val queueConsumer = Mockito.mock(QueueConsumer::class.java)
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.WRAP_IN_TRANSACTION).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        Mockito.`when`<Any>(queueConsumer.queueConfig).thenReturn(QueueConfig(location, settings))

        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            Mockito.mock(TaskLifecycleListener::class.java)
        )

        Assert.assertThat(
            queueRunner, CoreMatchers.instanceOf(
                QueueRunnerInTransaction::class.java
            )
        )
    }
}
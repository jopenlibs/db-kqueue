package ru.yoomoney.tech.dbqueue.internal.runner

import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.api.Task
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult.Companion.finish
import ru.yoomoney.tech.dbqueue.config.QueueShard
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner.Factory.create
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import ru.yoomoney.tech.dbqueue.settings.QueueId
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import ru.yoomoney.tech.dbqueue.stub.StringQueueConsumer
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer
import ru.yoomoney.tech.dbqueue.stub.TestFixtures
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
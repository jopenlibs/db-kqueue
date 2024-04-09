package io.github.jopenlibs.dbkqueue.internal.runner

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.api.Task
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult
import io.github.jopenlibs.dbkqueue.api.TaskExecutionResult.Companion.finish
import io.github.jopenlibs.dbkqueue.config.QueueShard
import io.github.jopenlibs.dbkqueue.config.QueueShardId
import io.github.jopenlibs.dbkqueue.internal.runner.QueueRunner.Factory.create
import io.github.jopenlibs.dbkqueue.settings.ProcessingMode
import io.github.jopenlibs.dbkqueue.settings.QueueConfig
import io.github.jopenlibs.dbkqueue.settings.QueueId
import io.github.jopenlibs.dbkqueue.settings.QueueLocation
import io.github.jopenlibs.dbkqueue.stub.StringQueueConsumer
import io.github.jopenlibs.dbkqueue.stub.StubDatabaseAccessLayer
import io.github.jopenlibs.dbkqueue.stub.TestFixtures
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
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
    fun should_return_external_executor_runner() {
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        val queueConsumer: QueueConsumer<*> = ConsumerWithExternalExecutor(
            QueueConfig(location, settings), mock()
        )
        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            mock()
        )

        assertThat(queueRunner).isInstanceOf(
            QueueRunnerInExternalExecutor::class.java
        )
    }

    @Test
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

        assertThrows<IllegalArgumentException> {
            create(
                queueConsumer,
                QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
                mock()
            )
        }
    }

    @Test
    fun should_return_separate_transactions_runner() {
        val queueConsumer: QueueConsumer<Any?> = mock()
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        whenever<Any>(queueConsumer.queueConfig).thenReturn(QueueConfig(location, settings))

        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            mock()
        )

        assertThat(queueRunner).isInstanceOf(
            QueueRunnerInSeparateTransactions::class.java
        )
    }

    @Test
    fun should_return_wrap_in_transaction_runner() {
        val queueConsumer: QueueConsumer<Any?> = mock()
        val settings = TestFixtures.createQueueSettings().withProcessingSettings(
            TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.WRAP_IN_TRANSACTION).build()
        ).build()
        val location = QueueLocation.builder().withTableName("testTable")
            .withQueueId(QueueId("testQueue")).build()
        whenever<Any>(queueConsumer.queueConfig).thenReturn(QueueConfig(location, settings))

        val queueRunner = create(
            queueConsumer,
            QueueShard(QueueShardId("s1"), StubDatabaseAccessLayer()),
            mock()
        )

        assertThat(queueRunner).isInstanceOf(
            QueueRunnerInTransaction::class.java
        )
    }
}
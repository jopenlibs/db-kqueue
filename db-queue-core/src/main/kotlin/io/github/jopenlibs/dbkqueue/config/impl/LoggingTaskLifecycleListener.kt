package io.github.jopenlibs.dbkqueue.config.impl

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult
import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.config.QueueShardId
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener
import ru.yoomoney.tech.dbqueue.settings.QueueLocation
import java.time.Clock
import java.time.Duration
import java.time.ZonedDateTime

/**
 * Task listener with logging support
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
class LoggingTaskLifecycleListener internal constructor(private val clock: Clock) : TaskLifecycleListener {
    /**
     * Constructor
     */
    constructor() : this(Clock.systemDefaultZone())

    override fun picked(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        pickTaskTime: Long
    ) {
    }

    override fun started(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    ) {
        log.info("consuming task: id={}, attempt={}", taskRecord.id, taskRecord.attemptsCount)
    }

    override fun executed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        executionResult: TaskExecutionResult, processTaskTime: Long
    ) {
        when (executionResult.actionType) {
            TaskExecutionResult.Type.FINISH -> {
                val inQueueTime = Duration.between(taskRecord.createdAt, ZonedDateTime.now(clock))
                log.info("task finished: id={}, in-queue={}, time={}", taskRecord.id, inQueueTime, processTaskTime)
            }

            TaskExecutionResult.Type.REENQUEUE -> log.info(
                "task reenqueued: id={}, delay={}, time={}", taskRecord.id,
                executionResult.getExecutionDelay().orElse(null), processTaskTime
            )

            TaskExecutionResult.Type.FAIL -> log.info("task failed: id={}, time={}", taskRecord.id, processTaskTime)
            else -> log.warn("unknown action type: type={}", executionResult.actionType)
        }
    }

    override fun finished(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord
    ) {
    }

    override fun crashed(
        shardId: QueueShardId,
        location: QueueLocation,
        taskRecord: TaskRecord,
        exc: Exception?
    ) {
        log.error("error while processing task: task={}", taskRecord, exc)
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(LoggingTaskLifecycleListener::class.java)
    }
}

package ru.yoomoney.tech.dbqueue.api

import ru.yoomoney.tech.dbqueue.settings.ProcessingMode
import ru.yoomoney.tech.dbqueue.settings.QueueConfig
import java.util.*
import java.util.concurrent.Executor

/**
 * Task processor for the queue
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 09.07.2017
</PayloadT> */
interface QueueConsumer<PayloadT> {
    /**
     * Process the task from the queue
     *
     * @param task A typed task for processing
     * @return A result of task processing
     */

    // TODO: fix generics at TaskProcessor.processTask
    // type must be PayloadT
    fun <T>execute(task: Task<T>): TaskExecutionResult

    val queueConfig: QueueConfig

    val payloadTransformer: TaskPayloadTransformer<PayloadT>

    /**
     * Task executor for [ProcessingMode.USE_EXTERNAL_EXECUTOR] mode.
     * Applies only to that mode
     *
     * @return [Optional] of external task executor
     */
    fun executor(): Optional<Executor> {
        return Optional.empty()
    }
}

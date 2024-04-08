package ru.yoomoney.tech.dbqueue.api

import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType
import java.time.Duration
import java.util.*


/**
 * The action, which should be performed after the task processing.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
class TaskExecutionResult private constructor(val actionType: Type, private val executionDelay: Duration? = null) {
    /**
     * Action performed after task processing
     */
    enum class Type {
        /**
         * Postpone (re-enqueue) the task, so that the task will be executed again
         */
        REENQUEUE,

        /**
         * Finish the task, task will be removed from the queue
         */
        FINISH,

        /**
         * Notify on error task execution, task will be postponed and executed again
         */
        FAIL
    }

    /**
     * Get task execution delay.
     *
     * @return task execution delay.
     */
    fun getExecutionDelay(): Optional<Duration> {
        return Optional.ofNullable(executionDelay)
    }

    val executionDelayOrThrow: Duration
        /**
         * Get task execution delay or throw an [IllegalStateException]
         * when task execution delay is not present.
         *
         * @return task execution delay.
         * @throws IllegalStateException An exception when task execution delay is not present.
         */
        get() {
            requireNotNull(executionDelay) { "executionDelay is absent" }
            return executionDelay
        }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as TaskExecutionResult
        return actionType == that.actionType && executionDelay == that.executionDelay
    }

    override fun hashCode(): Int {
        return Objects.hash(actionType, executionDelay)
    }

    override fun toString(): String {
        return '{'.toString() +
                "actionType=" + actionType +
                (if (executionDelay == null) "" else ", executionDelay=$executionDelay") +
                '}'
    }

    companion object {
        private val FINISH = TaskExecutionResult(Type.FINISH)
        private val FAIL = TaskExecutionResult(Type.FAIL)
        private val REENQUEUE_WITHOUT_DELAY = TaskExecutionResult(Type.REENQUEUE)

        /**
         * Instruction to re-enqueue the task with determined execution delay.
         * <br></br>
         * Re-enqueue attempts counter will be reset, task will be executed again after the given execution delay.
         *
         * @param delay determined execution delay, after which the task will be executed again.
         * @return Task execution action.
         */
        fun reenqueue(delay: Duration): TaskExecutionResult {
            return TaskExecutionResult(Type.REENQUEUE, delay)
        }

        /**
         * Instruction to re-enqueue the task using the [re-enqueue strategy][ReenqueueRetryType]
         * established in the queue configuration.
         * Re-enqueue attempts counter will be reset.
         *
         * @return Task execution action.
         */
        fun reenqueue(): TaskExecutionResult {
            return REENQUEUE_WITHOUT_DELAY
        }

        /**
         * Instruction to execute the task again later according to the [re-enqueue strategy][ReenqueueRetryType]
         * established in the queue configuration.
         *
         * @return Task execution action.
         */
        fun fail(): TaskExecutionResult {
            return FAIL
        }

        /**
         * Instruction to finish task processing and remove the task from the queue
         *
         * @return Task execution action.
         */
        fun finish(): TaskExecutionResult {
            return FINISH
        }
    }
}

package io.github.jopenlibs.dbkqueue.settings

/**
 * Type of the strategy, which computes the delay before
 * the next task execution if the task has to be brought back
 * [to the queue][TaskExecutionResult.Type.REENQUEUE].
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
enum class ReenqueueRetryType {
    /**
     * The task is deferred by the delay set manually with method
     * [TaskExecutionResult.reenqueue] call.
     *
     *
     * Default value for the task postponing strategy.
     *
     *
     * Settings example:
     * <pre>
     * `db-queue.queueName.reenqueue-retry-type=manual`
    </pre> *
     */
    MANUAL,

    /**
     * The task is deferred by the delay set with the sequence of delays.
     * Delay is selected from the sequence according
     * to [the number of task processing attempt][TaskRecord.getReenqueueAttemptsCount].
     * If the attempt number is bigger than the index of the last item in the sequence,
     * then the last item will be used.
     *
     *
     * For example, let the following sequence is set out in the settings:
     * <pre>
     * `db-queue.queueName.reenqueue-retry-type=sequential
     * db-queue.queueName.reenqueue-retry-plan=PT1S,PT10S,PT1M,P7D`
    </pre> *
     * For the first attempt to defer the task a delay of 1 second will be chosen (`PT1S`),
     * for the second one it will be 10 seconds and so forth.
     * For the fifth attempt and all the next after the delay will be 7 days.
     */
    SEQUENTIAL,

    /**
     * The task is deferred by the fixed delay, which is set in configuration.
     *
     *
     * Settings example:
     * <pre>
     * `db-queue.queueName.reenqueue-retry-type=fixed
     * db-queue.queueName.reenqueue-retry-delay=PT10S`
    </pre> *
     * Means that for each attempt the task will be deferred for 10 seconds.
     */
    FIXED,

    /**
     * The task is deferred by the delay set using an arithmetic progression.
     * The term of progression selected according
     * to [the number of attempt to postpone the task processing][TaskRecord.getReenqueueAttemptsCount].
     *
     *
     * The progression is set by a pair of values: the initial term (`reenqueue-retry-initial-delay`)
     * and the difference (`reenqueue-retry-step`).
     *
     *
     * Settings example:
     * <pre>
     * `db-queue.queueName.reenqueue-retry-type=arithmetic
     * db-queue.queueName.reenqueue-retry-initial-delay=PT1S
     * db-queue.queueName.reenqueue-retry-step=PT2S`
    </pre> *
     * Means that the task will be deferred with following delays: `1 second, 3 seconds, 5 seconds, 7 seconds, ...`
     */
    ARITHMETIC,

    /**
     * The task is deferred by the delay set using a geometric progression
     * The term of progression selected according to
     * [the number of attempt to postpone the task processing][TaskRecord.getReenqueueAttemptsCount].
     *
     *
     * The progression is set by a pair of values: the initial term and the integer denominator.
     *
     *
     * Settings example:
     * <pre>
     * `db-queue.queueName.reenqueue-retry-type=geometric
     * db-queue.queueName.reenqueue-retry-initial-delay=PT1S
     * db-queue.queueName.reenqueue-retry-ratio=2`
    </pre> *
     * Means that the task will be deferred with following delays: `1 second, 2 seconds, 4 seconds, 8 seconds, ...`
     */
    GEOMETRIC
}

package ru.yoomoney.tech.dbqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.BiFunction

/**
 * Settings for the task postponing strategy
 * when the task should be brought back to the queue.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
class ReenqueueSettings private constructor(
    /**
     * Strategy type, which computes delay to the next processing of the same task.
     */
    var retryType: ReenqueueRetryType,
    private var sequentialPlan: List<Duration>?,
    private var fixedDelay: Duration?,
    private var initialDelay: Duration?,
    private var arithmeticStep: Duration?,
    private var geometricRatio: Long?
) : DynamicSetting<ReenqueueSettings>() {

    init {
        require(!(retryType == ReenqueueRetryType.SEQUENTIAL && (sequentialPlan == null || sequentialPlan!!.isEmpty()))) { "sequentialPlan must not be empty when retryType=" + ReenqueueRetryType.SEQUENTIAL }
        require(!(retryType == ReenqueueRetryType.FIXED && fixedDelay == null)) { "fixedDelay must not be empty when retryType=" + ReenqueueRetryType.FIXED }
        require(!(retryType == ReenqueueRetryType.ARITHMETIC && (arithmeticStep == null || initialDelay == null))) { "arithmeticStep and initialDelay must not be empty when retryType=" + ReenqueueRetryType.ARITHMETIC }
        require(!(retryType == ReenqueueRetryType.GEOMETRIC && (geometricRatio == null || initialDelay == null))) { "geometricRatio and initialDelay must not be empty when retryType=" + ReenqueueRetryType.GEOMETRIC }    }

    val sequentialPlanOrThrow: List<Duration>
        /**
         * Get the sequential plan of delays for task processing.
         *
         *
         * Required when `type == ReenqueueRetryType.SEQUENTIAL`.
         *
         * @return Sequential plan of delays
         * @throws IllegalStateException when plan is not present.
         */
        get() {
            return sequentialPlan!!
        }

    val fixedDelayOrThrow: Duration
        /**
         * Fixed delay.
         *
         *
         * Required when `type == ReenqueueRetryType.FIXED`.
         *
         * @return Fixed delay.
         * @throws IllegalStateException when fixed delay is not present.
         */
        get() {
            checkNotNull(fixedDelay) { "fixed delay is null" }
            return fixedDelay!!
        }

    val initialDelayOrThrow: Duration
        /**
         * The first term of the progression to compute delays.
         *
         *
         * Required when `type == ReenqueueRetryType.ARITHMETIC` or `type == ReenqueueRetryType.GEOMETRIC`.
         *
         * @return initial delay
         * @throws IllegalStateException when initial delay is not present.
         */
        get() {
            checkNotNull(initialDelay) { "initial delay is null" }
            return initialDelay!!
        }

    val arithmeticStepOrThrow: Duration
        /**
         * The difference of the arithmetic progression.
         *
         *
         * Required when `type == ReenqueueRetryType.ARITHMETIC`.
         *
         * @return arithmetic step
         * @throws IllegalStateException when artithmetic step is not present.
         */
        get() {
            checkNotNull(arithmeticStep) { "arithmetic step is null" }
            return arithmeticStep!!
        }

    val geometricRatioOrThrow: Long
        /**
         * Denominator of the geometric progression.
         *
         *
         * Required when `type == ReenqueueRetryType.GEOMETRIC`.
         *
         * @return geometric ratio
         * @throws IllegalStateException when geometric ratio is not present.
         */
        get() {
            checkNotNull(geometricRatio) { "geometric ratio is null" }
            return geometricRatio!!
        }

    override fun toString(): String {
        return "{" +
                "retryType=" + retryType +
                ", sequentialPlan=" + sequentialPlan +
                ", fixedDelay=" + fixedDelay +
                ", initialDelay=" + initialDelay +
                ", arithmeticStep=" + arithmeticStep +
                ", geometricRatio=" + geometricRatio +
                '}'
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as ReenqueueSettings
        return retryType == that.retryType && sequentialPlan == that.sequentialPlan && fixedDelay == that.fixedDelay && initialDelay == that.initialDelay && arithmeticStep == that.arithmeticStep && geometricRatio == that.geometricRatio
    }

    override fun hashCode(): Int {
        return Objects.hash(retryType, sequentialPlan, fixedDelay, initialDelay, arithmeticStep, geometricRatio)
    }

    override val name: String
        get() = "reenqueueSettings"

    override val diffEvaluator: BiFunction<ReenqueueSettings, ReenqueueSettings, String>
        get() = BiFunction { oldVal: ReenqueueSettings, newVal: ReenqueueSettings ->
            val diff = StringJoiner(",", name + '(', ")")
            if (oldVal.retryType != newVal.retryType) {
                diff.add(
                    "type=" +
                            newVal.retryType + '<' + oldVal.retryType
                )
            }
            if (oldVal.arithmeticStep != newVal.arithmeticStep) {
                diff.add(
                    "arithmeticStep=" +
                            newVal.arithmeticStep + '<' + oldVal.arithmeticStep
                )
            }
            if (oldVal.geometricRatio != newVal.geometricRatio) {
                diff.add(
                    "geometricRatio=" +
                            newVal.geometricRatio + '<' + oldVal.geometricRatio
                )
            }
            if (oldVal.initialDelay != newVal.initialDelay) {
                diff.add(
                    "initialDelay=" +
                            newVal.initialDelay + '<' + oldVal.initialDelay
                )
            }
            if (oldVal.fixedDelay != newVal.fixedDelay) {
                diff.add(
                    "fixedDelay=" +
                            newVal.fixedDelay + '<' + oldVal.fixedDelay
                )
            }
            if (oldVal.sequentialPlan != newVal.sequentialPlan) {
                diff.add(
                    "sequentialPlan=" +
                            newVal.sequentialPlan + '<' + oldVal.sequentialPlan
                )
            }
            diff.toString()
        }

    override fun copyFields(newValue: ReenqueueSettings) {
        this.retryType = newValue.retryType
        this.arithmeticStep = newValue.arithmeticStep
        this.geometricRatio = newValue.geometricRatio
        this.fixedDelay = newValue.fixedDelay
        this.initialDelay = newValue.initialDelay
        this.sequentialPlan = newValue.sequentialPlan
    }

    /**
     * A builder for creating new instances of [ReenqueueSettings].
     */
    class Builder {
        private var retryType: ReenqueueRetryType? = null
        private var sequentialPlan: List<Duration>? = null
        private var fixedDelay: Duration? = null
        private var initialDelay: Duration? = null
        private var arithmeticStep: Duration? = null
        private var geometricRatio: Long? = null

        fun withRetryType(retryType: ReenqueueRetryType): Builder {
            this.retryType = retryType
            return this
        }

        fun withSequentialPlan(sequentialPlan: List<Duration>): Builder {
            this.sequentialPlan = sequentialPlan
            return this
        }

        fun withFixedDelay(fixedDelay: Duration): Builder {
            this.fixedDelay = fixedDelay
            return this
        }

        fun withInitialDelay(initialDelay: Duration): Builder {
            this.initialDelay = initialDelay
            return this
        }

        fun withArithmeticStep(arithmeticStep: Duration): Builder {
            this.arithmeticStep = arithmeticStep
            return this
        }

        fun withGeometricRatio(geometricRatio: Long): Builder {
            this.geometricRatio = geometricRatio
            return this
        }

        fun build(): ReenqueueSettings {
            return ReenqueueSettings(
                retryType!!,
                sequentialPlan,
                fixedDelay,
                initialDelay,
                arithmeticStep,
                geometricRatio
            )
        }
    }

    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }
}

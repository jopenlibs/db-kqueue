package io.github.jopenlibs.dbkqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.BiFunction

/**
 * Settings for task execution strategy in case of failure.
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
class FailureSettings private constructor(
    retryType: FailRetryType,
    retryInterval: Duration
) : DynamicSetting<FailureSettings>() {
    /**
     * Get task execution retry strategy in case of failure.
     *
     * @return Task execution retry strategy.
     */
    var retryType: FailRetryType
        private set

    /**
     * Get retry interval for task execution in case of failure.
     *
     * @return Task retry interval.
     * @see FailRetryType
     */
    var retryInterval: Duration
        private set

    init {
        this.retryType = Objects.requireNonNull(retryType, "retryType must not be null")
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval must not be null")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as FailureSettings
        return retryType == that.retryType && retryInterval == that.retryInterval
    }

    override fun hashCode(): Int {
        return Objects.hash(retryType, retryInterval)
    }

    override fun toString(): String {
        return "{" +
                "retryType=" + retryType +
                ", retryInterval=" + retryInterval +
                '}'
    }

    override val name: String
        get() = "failureSettings"

    override val diffEvaluator: BiFunction<FailureSettings, FailureSettings, String>
        get() = BiFunction { oldVal: FailureSettings, newVal: FailureSettings ->
            val diff = StringJoiner(",", name + '(', ")")
            if (oldVal.retryType != newVal.retryType) {
                diff.add(
                    "retryType=" +
                            newVal.retryType + '<' + oldVal.retryType
                )
            }
            if (oldVal.retryInterval != newVal.retryInterval) {
                diff.add(
                    "retryInterval=" +
                            newVal.retryInterval + '<' + oldVal.retryInterval
                )
            }
            diff.toString()
        }

    override fun copyFields(newValue: FailureSettings) {
        this.retryType = newValue.retryType
        this.retryInterval = newValue.retryInterval
    }

    /**
     * A builder for failure settings.
     */
    class Builder {
        private var retryType: FailRetryType? = null
        private var retryInterval: Duration? = null

        /**
         * Set task execution retry strategy in case of failure.
         *
         * @param retryType Task execution retry strategy.
         * @return Reference to the same builder.
         */
        fun withRetryType(retryType: FailRetryType): Builder {
            this.retryType = retryType
            return this
        }

        /**
         * Set retry interval for task execution in case of failure.
         *
         * @param retryInterval Task retry interval.
         * @return Reference to the same builder.
         */
        fun withRetryInterval(retryInterval: Duration): Builder {
            this.retryInterval = retryInterval
            return this
        }

        /**
         * Create new failure settings object.
         *
         * @return A new failure settings object.
         */
        fun build(): FailureSettings {
            return FailureSettings(retryType!!, retryInterval!!)
        }
    }

    companion object {
        /**
         * Create a new builder for failure settings.
         *
         * @return A new builder for failure settings.
         */
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

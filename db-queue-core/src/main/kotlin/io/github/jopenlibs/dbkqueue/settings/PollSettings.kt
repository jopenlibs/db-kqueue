package io.github.jopenlibs.dbkqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.BiFunction

/**
 * Task polling settings.
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
class PollSettings private constructor(
    betweenTaskTimeout: Duration,
    noTaskTimeout: Duration,
    fatalCrashTimeout: Duration
) : DynamicSetting<PollSettings>() {
    /**
     * Get delay duration between picking tasks from the queue after the task was processed.
     *
     * @return Delay after next task was processed.
     */
    var betweenTaskTimeout: Duration
        private set

    /**
     * Get delay duration between picking tasks from the queue if there are no task for processing.
     *
     * @return Delay when there are no tasks to process.
     */
    var noTaskTimeout: Duration
        private set

    /**
     * Get delay duration when task execution thread sleeps after unexpected error.
     *
     * @return Delay after unexpected error.
     */
    var fatalCrashTimeout: Duration
        private set

    init {
        this.betweenTaskTimeout = Objects.requireNonNull(betweenTaskTimeout, "betweenTaskTimeout must not be null")
        this.noTaskTimeout = Objects.requireNonNull(noTaskTimeout, "noTaskTimeout must not be null")
        this.fatalCrashTimeout = Objects.requireNonNull(fatalCrashTimeout, "fatalCrashTimeout must not be null")
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as PollSettings
        return betweenTaskTimeout == that.betweenTaskTimeout && noTaskTimeout == that.noTaskTimeout && fatalCrashTimeout == that.fatalCrashTimeout
    }

    override fun hashCode(): Int {
        return Objects.hash(betweenTaskTimeout, noTaskTimeout, fatalCrashTimeout)
    }

    override fun toString(): String {
        return "{" +
                "betweenTaskTimeout=" + betweenTaskTimeout +
                ", noTaskTimeout=" + noTaskTimeout +
                ", fatalCrashTimeout=" + fatalCrashTimeout +
                '}'
    }

    override val name: String
        get() = "pollSettings"

    override val diffEvaluator: BiFunction<PollSettings, PollSettings, String>
        get() = BiFunction { oldVal: PollSettings, newVal: PollSettings ->
            val diff = StringJoiner(",", name + '(', ")")
            if (oldVal.betweenTaskTimeout != newVal.betweenTaskTimeout) {
                diff.add(
                    "betweenTaskTimeout=" +
                            newVal.betweenTaskTimeout + '<' + oldVal.betweenTaskTimeout
                )
            }
            if (oldVal.noTaskTimeout != newVal.noTaskTimeout) {
                diff.add(
                    "noTaskTimeout=" +
                            newVal.noTaskTimeout + '<' + oldVal.noTaskTimeout
                )
            }
            if (oldVal.fatalCrashTimeout != newVal.fatalCrashTimeout) {
                diff.add(
                    "fatalCrashTimeout=" +
                            newVal.fatalCrashTimeout + '<' + oldVal.fatalCrashTimeout
                )
            }
            diff.toString()
        }

    override fun copyFields(newValue: PollSettings) {
        this.betweenTaskTimeout = newValue.betweenTaskTimeout
        this.noTaskTimeout = newValue.noTaskTimeout
        this.fatalCrashTimeout = newValue.fatalCrashTimeout
    }

    /**
     * A builder for poll settings.
     */
    class Builder {
        private var betweenTaskTimeout: Duration? = null
        private var noTaskTimeout: Duration? = null
        private var fatalCrashTimeout: Duration? = null

        /**
         * Set delay duration between picking tasks from the queue
         * after the task was processed.
         *
         * @param betweenTaskTimeout Delay after next task was processed.
         * @return Reference to the same builder.
         */
        fun withBetweenTaskTimeout(betweenTaskTimeout: Duration): Builder {
            this.betweenTaskTimeout = betweenTaskTimeout
            return this
        }

        /**
         * Set delay duration between picking tasks from the queue
         * if there are no task for processing.
         *
         * @param noTaskTimeout Delay when there are no tasks to process.
         * @return Reference to the same builder.
         */
        fun withNoTaskTimeout(noTaskTimeout: Duration): Builder {
            this.noTaskTimeout = noTaskTimeout
            return this
        }

        /**
         * Set delay duration after unexpected error.
         *
         * @param fatalCrashTimeout Delay after unexpected error.
         * @return Reference to the same builder.
         */
        fun withFatalCrashTimeout(fatalCrashTimeout: Duration): Builder {
            this.fatalCrashTimeout = fatalCrashTimeout
            return this
        }

        /**
         * Create new poll settings object.
         *
         * @return A new poll settings object.
         */
        fun build(): PollSettings {
            return PollSettings(betweenTaskTimeout!!, noTaskTimeout!!, fatalCrashTimeout!!)
        }
    }

    companion object {
        /**
         * Create a new builder for poll settings.
         *
         * @return A new builder for poll settings.
         */
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

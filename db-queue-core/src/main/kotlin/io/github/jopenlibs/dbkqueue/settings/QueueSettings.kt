package io.github.jopenlibs.dbkqueue.settings

import java.util.*

/**
 * Queue settings
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
class QueueSettings private constructor(
    val processingSettings: ProcessingSettings,
    val pollSettings: PollSettings,
    val failureSettings: FailureSettings,
    val reenqueueSettings: ReenqueueSettings,
    val extSettings: ExtSettings
) {

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as QueueSettings
        return processingSettings == that.processingSettings && pollSettings == that.pollSettings && failureSettings == that.failureSettings && reenqueueSettings == that.reenqueueSettings && extSettings == that.extSettings
    }

    override fun hashCode(): Int {
        return Objects.hash(processingSettings, pollSettings, failureSettings, reenqueueSettings, extSettings)
    }

    override fun toString(): String {
        return "{" +
                "processingSettings=" + processingSettings +
                ", pollSettings=" + pollSettings +
                ", failureSettings=" + failureSettings +
                ", reenqueueSettings=" + reenqueueSettings +
                ", additionalSettings=" + extSettings +
                '}'
    }

    /**
     * A builder for queue settings.
     */
    class Builder {
        private var processingSettings: ProcessingSettings? = null
        private var pollSettings: PollSettings? = null
        private var failureSettings: FailureSettings? = null
        private var reenqueueSettings: ReenqueueSettings? = null
        private var extSettings: ExtSettings? = null

        /**
         * Sets task processing settings.
         *
         * @param processingSettings processing settings
         * @return Reference to the same builder.
         */
        fun withProcessingSettings(processingSettings: ProcessingSettings): Builder {
            this.processingSettings = processingSettings
            return this
        }

        /**
         * Sets task polling settings
         *
         * @param pollSettings poll settings
         * @return Reference to the same builder.
         */
        fun withPollSettings(pollSettings: PollSettings): Builder {
            this.pollSettings = pollSettings
            return this
        }

        /**
         * Sets settings for task execution strategy in case of failure.
         *
         * @param failureSettings fail postpone settings
         * @return Reference to the same builder.
         */
        fun withFailureSettings(failureSettings: FailureSettings): Builder {
            this.failureSettings = failureSettings
            return this
        }

        /**
         * Set Settings for the task postponing strategy
         * when the task should be brought back to the queue.
         *
         * @param reenqueueSettings Task postponing settings.
         * @return Reference to the same builder.
         */
        fun withReenqueueSettings(reenqueueSettings: ReenqueueSettings): Builder {
            this.reenqueueSettings = reenqueueSettings
            return this
        }

        /**
         * Set the map of additional properties for the queue.
         *
         * @param extSettings Additional properties for the queue.
         * @return Reference to the same builder.
         */
        fun withExtSettings(extSettings: ExtSettings): Builder {
            this.extSettings = extSettings
            return this
        }

        /**
         * Create new queue settings object.
         *
         * @return A new queue settings object.
         */
        fun build(): QueueSettings {
            return QueueSettings(
                processingSettings!!,
                pollSettings!!,
                failureSettings!!,
                reenqueueSettings!!,
                extSettings!!
            )
        }
    }

    companion object {
        /**
         * Create a new builder for queue settings.
         *
         * @return A new builder for queue settings.
         */
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

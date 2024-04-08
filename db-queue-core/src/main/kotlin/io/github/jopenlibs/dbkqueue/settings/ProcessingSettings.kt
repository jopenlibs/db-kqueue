package io.github.jopenlibs.dbkqueue.settings

import java.util.*
import java.util.function.BiFunction

/**
 * Task processing settings.
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
class ProcessingSettings private constructor(
    /**
     *  Number of threads for processing tasks in the queue.
     */
    var threadCount: Int,
    /**
     * Task processing mode in the queue.
     */
    var processingMode: ProcessingMode
) : DynamicSetting<ProcessingSettings>() {

    init {
        require(threadCount >= 0) { "threadCount must not be negative" }
    }

    override val name: String
        get() = "processingSettings"

    override val diffEvaluator: BiFunction<ProcessingSettings, ProcessingSettings, String>
        get() = BiFunction { oldVal: ProcessingSettings, newVal: ProcessingSettings ->
            val diff = StringJoiner(",", name + '(', ")")
            if (oldVal.threadCount != newVal.threadCount) {
                diff.add(
                    "threadCount=" +
                            newVal.threadCount + '<' + oldVal.threadCount
                )
            }
            if (oldVal.processingMode != newVal.processingMode) {
                diff.add(
                    "processingMode=" +
                            newVal.processingMode + '<' + oldVal.processingMode
                )
            }
            diff.toString()
        }


    override fun copyFields(newValue: ProcessingSettings) {
        this.threadCount = newValue.threadCount
        this.processingMode = newValue.processingMode
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as ProcessingSettings
        return threadCount == that.threadCount && processingMode == that.processingMode
    }

    override fun hashCode(): Int {
        return Objects.hash(threadCount, processingMode)
    }

    override fun toString(): String {
        return "{" +
                "threadCount=" + threadCount +
                ", processingMode=" + processingMode +
                '}'
    }

    /**
     * A builder for processing settings.
     */
    class Builder {
        private var threadCount: Int? = null
        private var processingMode: ProcessingMode? = null

        /**
         * Set number of threads for processing tasks in the queue.
         *
         * @param threadCount Number of processing threads.
         * @return Reference to the same builder.
         */
        fun withThreadCount(threadCount: Int): Builder {
            this.threadCount = threadCount
            return this
        }

        /**
         * Set task processing mode in the queue.
         *
         * @param processingMode Task processing mode.
         * @return Reference to the same builder.
         */
        fun withProcessingMode(processingMode: ProcessingMode): Builder {
            this.processingMode = processingMode
            return this
        }

        fun build(): ProcessingSettings {
            return ProcessingSettings(threadCount!!, processingMode!!)
        }
    }

    companion object {
        /**
         * Create a new builder for processing settings.
         *
         * @return A new builder for processing settings.
         */
        fun builder(): Builder {
            return Builder()
        }
    }
}

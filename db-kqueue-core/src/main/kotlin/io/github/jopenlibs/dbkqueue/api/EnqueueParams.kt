package io.github.jopenlibs.dbkqueue.api

import java.time.Duration
import java.util.*

/**
 * Parameters with typed payload to enqueue the task
 *
 * @param <PayloadT> A type of the payload in the task
 * @author Oleg Kandaurov
 * @since 12.07.2017
</PayloadT> */
class EnqueueParams<PayloadT> {
    /**
     * Get task payload
     *
     * @return Typed task payload
     */
    var payload: PayloadT? = null
        private set

    /**
     * Get the task execution delay, a [Duration.ZERO] is the default one if not set.
     *
     * @return Task execution delay.
     */
    var executionDelay: Duration = Duration.ZERO
        private set

    private val extData: MutableMap<String, String?> = LinkedHashMap()

    /**
     * Add a typed payload to the task parameters
     *
     * @param payload Task payload
     * @return A reference to the same object with added payload
     */
    fun withPayload(payload: PayloadT?): io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadT> {
        this.payload = payload
        return this
    }

    /**
     * Add an execution delay for the task.
     * The given task will not be executed before current date and time plus the execution delay.
     *
     * @param executionDelay Execution delay, [Duration.ZERO] if not set.
     * @return A reference to the same object with execution delay set.
     */
    fun withExecutionDelay(executionDelay: Duration): io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadT> {
        this.executionDelay = executionDelay
        return this
    }

    /**
     * Add the external user parameter for the task.
     * If the column name is already present in the external user parameters,
     * then the original value will be replaced by the new one.
     *
     * @param columnName The name of the user-defined column in tasks table.
     * The column **must** exist in the tasks table.
     * @param value      The value of the user-defined parameter
     * @return A reference to the same object of the task parameters with external user parameter.
     */
    fun withExtData(columnName: String, value: String?): io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadT> {
        extData[columnName] = value
        return this
    }

    /**
     * Update the task parameters with the map of external user-defined parameters,
     * a map where the key is the name of the user-defined column in tasks table,
     * and the value is the value of the user-defined parameter.
     *
     * @param extData Map of external user-defined parameters, key is the column name in the tasks table.
     * All elements of that collection will be **added** to those
     * already present in task parameters object,
     * the value will replace the existing value on a duplicate key.
     * @return A reference to the same object of the task parameters with external user-defined parameters map.
     */
    fun withExtData(extData: Map<String, String?>): io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadT> {
        this.extData.putAll(extData)
        return this
    }

    /**
     * Get the **unmodifiable** map of extended user-defined parameters for the task:
     * a map where the key is the name of the user-defined column in tasks table,
     * and the value is the value of the user-defined parameter.
     *
     * @return Map of external user-defined parameters, where the key is the column name in the tasks table.
     */
    fun getExtData(): Map<String, String?> {
        return Collections.unmodifiableMap(extData)
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as io.github.jopenlibs.dbkqueue.api.EnqueueParams<*>
        return payload == that.payload && executionDelay == that.executionDelay && extData == that.extData
    }

    override fun hashCode(): Int {
        return Objects.hash(payload, executionDelay, extData)
    }

    override fun toString(): String {
        return '{'.toString() +
                "executionDelay=" + executionDelay +
                (if (payload != null) ",payload=$payload" else "") +
                '}'
    }

    companion object {
        /**
         * Create new task parameters with payload
         *
         * @param payload    task payload
         * @param <PayloadBuilderT> The type of the payload in the task
         * @return An object with task parameters and a payload
        </PayloadBuilderT> */
        fun <PayloadBuilderT> create(payload: PayloadBuilderT): io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadBuilderT> {
            Objects.requireNonNull(payload)
            return io.github.jopenlibs.dbkqueue.api.EnqueueParams<PayloadBuilderT>().withPayload(payload)
        }
    }
}

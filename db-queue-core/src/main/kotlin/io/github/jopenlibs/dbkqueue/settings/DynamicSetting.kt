package io.github.jopenlibs.dbkqueue.settings

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.BiConsumer
import java.util.function.BiFunction
import java.util.function.Consumer

/**
 * Base class for dynamic settings.
 *
 *
 * Use it when you need track changes in some setting.
 *
 * @param <T> type of setting
 * @author Oleg Kandaurov
 * @since 01.10.2021
</T> */
abstract class DynamicSetting<T> {
    private val observers: MutableCollection<BiConsumer<T, T>> = CopyOnWriteArrayList()

    protected abstract val name: String
        /**
         * Name of setting
         *
         * @return name
         */
        get

    protected abstract val diffEvaluator: BiFunction<T, T, String>
        /**
         * Function evaluates difference between new and old value.
         * 1st argument - old value, 2nd argument - new value.
         *
         * @return difference between two values
         */
        get

    /**
     * Copy fields of new object to current object.
     *
     * @param newValue new value
     */
    protected abstract fun copyFields(newValue: T)

    /**
     * Sets new value for current setting.
     * Notifies observer when property is changed.
     *
     * @param newValue new value for setting
     * @return diff between old value and new value. Returns empty object when no changes detected.
     * @see DynamicSetting.registerObserver
     */
    @Suppress("UNCHECKED_CAST")
    fun setValue(newValue: T): Optional<String> {
        val oldValue = this as T
        try {
            Objects.requireNonNull(newValue, name + " must not be null")
            if (newValue == oldValue) {
                return Optional.empty()
            }
            observers.forEach(Consumer { observer: BiConsumer<T, T> -> observer.accept(oldValue, newValue) })
            val diff = diffEvaluator.apply(oldValue, newValue)
            copyFields(newValue)
            return Optional.of(diff)
        } catch (exc: RuntimeException) {
            log.error(
                "Cannot apply new setting: name={}, oldValue={}, newValue={}",
                name, oldValue, newValue, exc
            )
            return Optional.empty()
        }
    }

    /**
     * Register observer to track setting changes.
     *
     * @param observer consumer which will be notified on property change.
     * 1st argument - old value, 2nd argument - new value
     */
    fun registerObserver(observer: BiConsumer<T, T>) {
        observers.add(observer)
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(DynamicSetting::class.java)
    }
}

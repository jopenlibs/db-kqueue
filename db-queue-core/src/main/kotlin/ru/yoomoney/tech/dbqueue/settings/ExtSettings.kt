package ru.yoomoney.tech.dbqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.BiFunction
import java.util.function.Consumer

/**
 * Additional custom settings
 */
class ExtSettings internal constructor(private var extSettings: Map<String, String>) : DynamicSetting<ExtSettings>() {
    override val name: String
        get() = "extSettings"

    override val diffEvaluator: BiFunction<ExtSettings, ExtSettings, String>
        get() = BiFunction { oldValue: ExtSettings, newValue: ExtSettings ->
            val sameEntries: MutableCollection<String> = LinkedHashSet(
                newValue.extSettings.keys
            )
            sameEntries.retainAll(oldValue.extSettings.keys)

            val entriesInNew: MutableCollection<String> = LinkedHashSet(
                newValue.extSettings.keys
            )
            entriesInNew.removeAll(oldValue.extSettings.keys)

            val entriesInOld: MutableCollection<String> = LinkedHashSet(
                oldValue.extSettings.keys
            )
            entriesInOld.removeAll(newValue.extSettings.keys)

            val diff = StringJoiner(",", name + '(', ")")
            sameEntries.forEach(Consumer { key: String -> diff.add(key + '=' + newValue.extSettings[key] + '<' + oldValue.extSettings[key]) })
            entriesInNew.forEach(Consumer { key: String -> diff.add(key + '=' + newValue.extSettings[key] + '<' + null) })
            entriesInOld.forEach(Consumer { key: String -> diff.add(key + '=' + null + '<' + oldValue.extSettings[key]) })
            diff.toString()
        }

    override fun copyFields(newValue: ExtSettings) {
        this.extSettings = newValue.extSettings
    }

    /**
     * Get [Duration] value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    fun getDurationProperty(settingName: String): Duration {
        return Duration.parse(getProperty(settingName))
    }

    /**
     * Get string value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    fun getProperty(settingName: String): String {
        return extSettings[settingName]
            ?: throw NullPointerException(String.format("null values are not allowed: settingName=%s", settingName))
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || javaClass != obj.javaClass) {
            return false
        }
        val that = obj as ExtSettings
        return extSettings == that.extSettings
    }

    override fun hashCode(): Int {
        return Objects.hash(extSettings)
    }


    override fun toString(): String {
        return extSettings.toString()
    }

    /**
     * A builder for ext settings.
     */
    class Builder {
        private var extSettings: Map<String, String>? = null

        fun withSettings(extSettings: Map<String, String>): Builder {
            this.extSettings = extSettings
            return this
        }

        fun build(): ExtSettings {
            return ExtSettings(extSettings!!)
        }
    }

    companion object {
        /**
         * Create a new builder for ext settings.
         *
         * @return A new builder for ext settings.
         */
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

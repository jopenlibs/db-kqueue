package ru.yoomoney.tech.dbqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.Supplier

/**
 * Parser for [FailureSettings]
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
internal class FailureSettingsParser(
    private val defaultSettings: Supplier<FailureSettings.Builder>,
    private val errorMessages: MutableList<String>
) {
    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    fun parseSettings(queueId: String, settings: Map<String, String>): Optional<FailureSettings> {
        Objects.requireNonNull(queueId, "queueId")
        Objects.requireNonNull(settings, "settings")
        try {
            val failureSettings = defaultSettings.get()
            settings.forEach { (key: String, value: String) -> fillSettings(failureSettings, key, value) }
            return Optional.of(failureSettings.build())
        } catch (exc: RuntimeException) {
            errorMessages.add(String.format("cannot build failure settings: queueId=%s, msg=%s", queueId, exc.message))
            return Optional.empty()
        }
    }

    private fun fillSettings(failureSettings: FailureSettings.Builder, name: String, value: String) {
        try {
            when (name) {
                QueueConfigsReader.SETTING_RETRY_TYPE -> {
                    failureSettings.withRetryType(parseRetryType(value))
                    return
                }

                QueueConfigsReader.SETTING_RETRY_INTERVAL -> {
                    failureSettings.withRetryInterval(Duration.parse(value))
                    return
                }

                else -> return

            }
        } catch (exc: RuntimeException) {
            errorMessages.add(
                String.format(
                    "cannot parse setting: name=%s, value=%s, exception=%s", name, value,
                    exc.javaClass.simpleName + '(' + exc.message + ')'
                )
            )
        }
    }

    companion object {
        private fun parseRetryType(name: String): FailRetryType {
            return when (name) {
                QueueConfigsReader.VALUE_TASK_RETRY_TYPE_GEOMETRIC -> FailRetryType.GEOMETRIC_BACKOFF
                QueueConfigsReader.VALUE_TASK_RETRY_TYPE_ARITHMETIC -> FailRetryType.ARITHMETIC_BACKOFF
                QueueConfigsReader.VALUE_TASK_RETRY_TYPE_LINEAR -> FailRetryType.LINEAR_BACKOFF
                else -> throw IllegalArgumentException(String.format("unknown retry type: name=%s", name))
            }
        }
    }
}

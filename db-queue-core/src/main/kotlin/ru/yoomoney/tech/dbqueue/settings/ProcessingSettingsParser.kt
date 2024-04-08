package ru.yoomoney.tech.dbqueue.settings

import java.util.*
import java.util.function.Supplier

/**
 * Parser for [ProcessingSettings]
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
class ProcessingSettingsParser internal constructor(
    private val defaultSettings: Supplier<ProcessingSettings.Builder>,
    private val errorMessages: MutableList<String>
) {
    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    fun parseSettings(queueId: String, settings: Map<String, String>): Optional<ProcessingSettings> {
        Objects.requireNonNull(queueId, "queueId")
        Objects.requireNonNull(settings, "settings")
        try {
            val processingSettings = defaultSettings.get()
            settings.forEach { (key: String, value: String) -> fillSettings(processingSettings, key, value) }
            return Optional.of(processingSettings.build())
        } catch (exc: RuntimeException) {
            errorMessages.add(
                String.format(
                    "cannot build processing settings: queueId=%s, msg=%s",
                    queueId,
                    exc.message
                )
            )
            return Optional.empty()
        }
    }

    private fun fillSettings(processingSettings: ProcessingSettings.Builder, name: String, value: String) {
        try {
            when (name) {
                QueueConfigsReader.SETTING_THREAD_COUNT -> {
                    processingSettings.withThreadCount(value.toInt())
                    return
                }

                QueueConfigsReader.SETTING_PROCESSING_MODE -> {
                    processingSettings.withProcessingMode(parseProcessingMode(value))
                    return
                }

                else -> {}
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
        private fun parseProcessingMode(name: String): ProcessingMode {
            return when (name) {
                QueueConfigsReader.Companion.VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS -> ProcessingMode.SEPARATE_TRANSACTIONS
                QueueConfigsReader.Companion.VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION -> ProcessingMode.WRAP_IN_TRANSACTION
                QueueConfigsReader.Companion.VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR -> ProcessingMode.USE_EXTERNAL_EXECUTOR
                else -> throw IllegalArgumentException(String.format("unknown processing mode: name=%s", name))
            }
        }
    }
}

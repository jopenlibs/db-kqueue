package io.github.jopenlibs.dbkqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.Supplier

/**
 * Parser for [PollSettings]
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
internal class PollSettingsParser
/**
 * Constructor
 *
 * @param defaultSettings default settings
 * @param errorMessages   list of error messages
 */(private val defaultSettings: Supplier<PollSettings.Builder>, private val errorMessages: MutableList<String>) {
    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    fun parseSettings(queueId: String, settings: Map<String, String>): Optional<PollSettings> {
        Objects.requireNonNull(queueId, "queueId")
        Objects.requireNonNull(settings, "settings")
        try {
            val pollSettings = defaultSettings.get()
            settings.forEach { (key: String, value: String) -> fillSettings(pollSettings, key, value) }
            return Optional.of(pollSettings.build())
        } catch (exc: RuntimeException) {
            errorMessages.add(String.format("cannot build poll settings: queueId=%s, msg=%s", queueId, exc.message))
            return Optional.empty()
        }
    }

    private fun fillSettings(pollSettings: PollSettings.Builder, name: String, value: String) {
        try {
            when (name) {
                QueueConfigsReader.SETTING_NO_TASK_TIMEOUT -> {
                    pollSettings.withNoTaskTimeout(Duration.parse(value))
                    return
                }

                QueueConfigsReader.SETTING_BETWEEN_TASK_TIMEOUT -> {
                    pollSettings.withBetweenTaskTimeout(Duration.parse(value))
                    return
                }

                QueueConfigsReader.SETTING_FATAL_CRASH_TIMEOUT -> {
                    pollSettings.withFatalCrashTimeout(Duration.parse(value))
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
}

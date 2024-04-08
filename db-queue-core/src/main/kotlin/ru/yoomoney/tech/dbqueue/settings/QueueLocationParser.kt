package ru.yoomoney.tech.dbqueue.settings

import java.util.*

/**
 * Parser for [QueueLocation]
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
internal class QueueLocationParser(private val errorMessages: MutableList<String>) {
    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    fun parseQueueLocation(queueId: String, settings: Map<String, String>): Optional<QueueLocation> {
        Objects.requireNonNull(queueId, "queueId")
        Objects.requireNonNull(settings, "settings")
        try {
            val queueLocation: QueueLocation.Builder = QueueLocation.Companion.builder()
            queueLocation.withQueueId(QueueId(queueId))
            settings.forEach { (key: String, value: String) -> fillSettings(queueLocation, key, value) }
            return Optional.of(queueLocation.build())
        } catch (exc: RuntimeException) {
            errorMessages.add(String.format("cannot build queue location: queueId=%s, msg=%s", queueId, exc.message))
            return Optional.empty()
        }
    }

    private fun fillSettings(queueLocation: QueueLocation.Builder, name: String, value: String) {
        try {
            when (name) {
                QueueConfigsReader.SETTING_TABLE -> {
                    queueLocation.withTableName(value)
                    return
                }

                QueueConfigsReader.SETTING_ID_SEQUENCE -> {
                    queueLocation.withIdSequence(value)
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

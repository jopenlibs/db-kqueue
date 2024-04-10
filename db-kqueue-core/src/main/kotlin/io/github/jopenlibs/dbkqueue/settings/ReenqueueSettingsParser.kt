package io.github.jopenlibs.dbkqueue.settings

import java.time.Duration
import java.util.*
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * Parser for [ReenqueueSettings]
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
internal class ReenqueueSettingsParser(
    private val defaultSettings: Supplier<ReenqueueSettings.Builder>,
    private val errorMessages: MutableList<String>
) {
    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    fun parseSettings(queueId: String, settings: Map<String, String>): Optional<ReenqueueSettings> {
        try {
            val reenqueueSettings = defaultSettings.get()
            settings.forEach { (key: String, value: String) -> fillSettings(reenqueueSettings, key, value) }
            return Optional.of(reenqueueSettings.build())
        } catch (exc: RuntimeException) {
            errorMessages.add(
                String.format(
                    "cannot build reenqueue settings: queueId=%s, msg=%s",
                    queueId,
                    exc.message
                )
            )
            return Optional.empty()
        }
    }

    private fun fillSettings(builder: ReenqueueSettings.Builder, name: String, value: String) {
        try {
            when (name) {
                QueueConfigsReader.Companion.SETTING_REENQUEUE_RETRY_TYPE -> {
                    builder.withRetryType(parseReenqueueRetryType(value))
                    return
                }

                QueueConfigsReader.Companion.SETTING_REENQUEUE_RETRY_PLAN -> {
                    builder.withSequentialPlan(parseReenqueueRetryPlan(value))
                    return
                }

                QueueConfigsReader.Companion.SETTING_REENQUEUE_RETRY_DELAY -> {
                    builder.withFixedDelay(Duration.parse(value))
                    return
                }

                QueueConfigsReader.Companion.SETTING_REENQUEUE_RETRY_INITIAL_DELAY -> {
                    builder.withInitialDelay(Duration.parse(value))
                    return
                }

                QueueConfigsReader.Companion.SETTING_REENQUEUE_RETRY_STEP -> {
                    builder.withArithmeticStep(Duration.parse(value))
                    return
                }

                QueueConfigsReader.Companion.SETTING_REENQUEUE_RETRY_RATIO -> {
                    builder.withGeometricRatio(value.toLong())
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
        private fun parseReenqueueRetryType(type: String): ReenqueueRetryType {
            return when (type) {
                QueueConfigsReader.Companion.VALUE_REENQUEUE_RETRY_TYPE_MANUAL -> ReenqueueRetryType.MANUAL
                QueueConfigsReader.Companion.VALUE_REENQUEUE_RETRY_TYPE_FIXED -> ReenqueueRetryType.FIXED
                QueueConfigsReader.Companion.VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL -> ReenqueueRetryType.SEQUENTIAL
                QueueConfigsReader.Companion.VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC -> ReenqueueRetryType.ARITHMETIC
                QueueConfigsReader.Companion.VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC -> ReenqueueRetryType.GEOMETRIC
                else -> throw IllegalArgumentException(
                    String.format(
                        "unknown reenqueue retry type: type=%s",
                        type
                    )
                )
            }
        }

        private fun parseReenqueueRetryPlan(plan: String): List<Duration> {
            val values = plan.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            return Arrays.stream(values)
                .map { text: String? -> Duration.parse(text) }
                .collect(Collectors.toList())
        }
    }
}

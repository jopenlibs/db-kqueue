package ru.yoomoney.tech.dbqueue.settings

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.function.Function
import java.util.function.Supplier
import java.util.regex.Pattern
import java.util.stream.Collectors

/**
 * Reads queue configuration from file.
 *
 *
 * File should be a regular [Properties] file, so format is 'queue-prefix.key.innerkey=val'.
 * Where actual name of 'queue-prefix' is defined in constructor. Settings with other prefixes are ignored.
 *
 *  * Key names of queue settings should contain alphanumeric characters, dashes and underscores.
 *  * Duration settings must be in ISO-8601. see [Duration.parse]
 *
 *
 *
 * Full configuration looks like:
 * <pre>
 * # see [QueueConfigsReader.SETTING_TABLE]
 * queue-prefix.testQueue.table=foo
 *
 * # see [QueueConfigsReader.SETTING_BETWEEN_TASK_TIMEOUT]
 * queue-prefix.testQueue.between-task-timeout=PT0.1S
 *
 * # see [QueueConfigsReader.SETTING_NO_TASK_TIMEOUT]
 * queue-prefix.testQueue.no-task-timeout=PT1S
 *
 * # see [QueueConfigsReader.SETTING_FATAL_CRASH_TIMEOUT]
 * queue-prefix.testQueue.fatal-crash-timeout=PT5S
 *
 * # see [QueueConfigsReader.SETTING_THREAD_COUNT]
 * queue-prefix.testQueue.thread-count=3
 *
 * # see [QueueConfigsReader.SETTING_RETRY_TYPE]
 * # values are:
 * # [QueueConfigsReader.VALUE_TASK_RETRY_TYPE_ARITHMETIC]
 * # [QueueConfigsReader.VALUE_TASK_RETRY_TYPE_GEOMETRIC]
 * # [QueueConfigsReader.VALUE_TASK_RETRY_TYPE_LINEAR]
 * queue-prefix.testQueue.retry-type=linear
 *
 * # see [QueueConfigsReader.SETTING_RETRY_INTERVAL]
 * queue-prefix.testQueue.retry-interval=PT30S
 *
 * # see [QueueConfigsReader.SETTING_REENQUEUE_RETRY_TYPE]
 * # values are:
 * # [QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_MANUAL]
 * # [QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_FIXED]
 * # [QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL]
 * # [QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC]
 * # [QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC]
 * #
 * # [QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_MANUAL] is used by default
 * queue-prefix.testQueue.reenqueue-retry-type=fixed
 *
 * # see [QueueConfigsReader.SETTING_REENQUEUE_RETRY_DELAY]
 * # Required when [QueueConfigsReader.SETTING_REENQUEUE_RETRY_TYPE] is set to 'fixed'
 * queue-prefix.testQueue.reenqueue-retry-delay=PT10S
 *
 * # see [QueueConfigsReader.SETTING_REENQUEUE_RETRY_PLAN]
 * # Required when [QueueConfigsReader.SETTING_REENQUEUE_RETRY_TYPE] is set to 'sequential'
 * queue-prefix.testQueue.reenqueue-retry-plan=PT1S,PT2S,PT3S
 *
 * # see [QueueConfigsReader.SETTING_REENQUEUE_RETRY_INITIAL_DELAY]
 * # PT1S is used by default.
 * queue-prefix.testQueue.reenqueue-retry-initial-delay=PT10S
 *
 * # see [QueueConfigsReader.SETTING_REENQUEUE_RETRY_STEP]
 * # PT2S is used by default.
 * queue-prefix.testQueue.reenqueue-retry-step=PT2S
 *
 * # see [QueueConfigsReader.SETTING_REENQUEUE_RETRY_RATIO]
 * # 2 is used by default.
 * queue-prefix.testQueue.reenqueue-retry-ratio=3
 *
 * # see [QueueConfigsReader.SETTING_PROCESSING_MODE]
 * # values are:
 * # [QueueConfigsReader.VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS]
 * # [QueueConfigsReader.VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR]
 * # [QueueConfigsReader.VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION]
 * queue-prefix.testQueue.processing-mode=use-external-executor
 *
 * # see [QueueConfigsReader.SETTING_ADDITIONAL]
 * # see [QueueSettings.getExtSettings]
 * queue-prefix.testQueue.additional-settings.custom-val=custom-key
 *
 * # you can define custom settings to use it in enqueueing or processing
 * queue-prefix.testQueue.additional-settings.custom=val1
</pre> *
 *
 *
 * Where 'testQueue' is name of a queue you apply configuration to.
 *
 * @author Oleg Kandaurov
 * @see QueueSettings
 *
 * @see QueueLocation
 *
 * @see FailRetryType
 *
 * @see ProcessingMode
 *
 * @see QueueConfig
 *
 * @since 22.08.2017
 */
class QueueConfigsReader @JvmOverloads constructor(
    private val configPaths: List<Path>,
    private val settingsPrefix: String,
    private val defaultProcessingSettings: Supplier<ProcessingSettings.Builder> = Supplier<ProcessingSettings.Builder> { ProcessingSettings.builder() },
    private val defaultPollSettings: Supplier<PollSettings.Builder> = Supplier<PollSettings.Builder> { PollSettings.builder() },
    private val defaultFailureSettings: Supplier<FailureSettings.Builder> = Supplier<FailureSettings.Builder> { FailureSettings.builder() },
    private val defaultReenqueueSettings: Supplier<ReenqueueSettings.Builder> = Supplier<ReenqueueSettings.Builder> { ReenqueueSettings.builder() }
) {
    private val errorMessages: MutableList<String> = ArrayList()

    /**
     * Constructor
     *
     * @param configPaths               files to read configuration from.
     * @param settingsPrefix            prefix that will be used for queue settings.
     * @param defaultProcessingSettings default [ProcessingSettings]
     * @param defaultPollSettings       default [PollSettings]
     * @param defaultFailureSettings    default [FailureSettings]
     * @param defaultReenqueueSettings  default [ReenqueueSettings]
     */
    /**
     * Constructor
     *
     * @param configPaths    files to read configuration from.
     * @param settingsPrefix prefix that will be used for queue settings.
     */
    init {
        require(!configPaths.isEmpty()) { "config paths must not be empty" }
        val illegalConfigs =
            configPaths.stream().filter { path: Path? -> !path!!.toFile().isFile }.collect(Collectors.toList())
        require(illegalConfigs.isEmpty()) { "config path must be a file: files=$illegalConfigs" }
    }

    /**
     * Get paths to queue configs
     *
     * @return paths to queue configs
     */
    fun getConfigPaths(): List<Path> {
        return ArrayList(configPaths)
    }

    /**
     * Try to parse queues configurations.
     *
     * @return parsed queue configurations
     */
    fun parse(): List<QueueConfig> {
        log.info("loading queue configuration: paths={}", configPaths)
        val configPath = configPaths[0]
        val rawSettings = readRawSettings(configPath)
        if (configPaths.size > 1) {
            val overrideConfigPaths = configPaths.subList(1, configPaths.size)
            overrideConfigPaths.stream().filter { obj: Path? -> Objects.nonNull(obj) }
                .forEach { path: Path? -> overrideExistingSettings(rawSettings, readRawSettings(path)) }
        }

        val queues = splitRawSettingsByQueueId(rawSettings)

        val queueLocationParser = QueueLocationParser(errorMessages)
        val processingSettingsParser = ProcessingSettingsParser(
            defaultProcessingSettings,
            errorMessages
        )
        val pollSettingsParser = PollSettingsParser(
            defaultPollSettings,
            errorMessages
        )
        val reenqueueSettingsParser = ReenqueueSettingsParser(
            defaultReenqueueSettings,
            errorMessages
        )
        val failureSettingsParser = FailureSettingsParser(
            defaultFailureSettings,
            errorMessages
        )

        val queueConfigs: MutableList<QueueConfig> = ArrayList()
        queues.forEach { (queueId: String, settings: Map<String, String>) ->
            val queueLocation = queueLocationParser.parseQueueLocation(queueId, settings)
            val processingSettings = processingSettingsParser.parseSettings(queueId, settings)
            val pollSettings = pollSettingsParser.parseSettings(queueId, settings)
            val failureSettings = failureSettingsParser.parseSettings(queueId, settings)
            val reenqueueSettings = reenqueueSettingsParser.parseSettings(queueId, settings)
            if (queueLocation.isPresent && processingSettings.isPresent && pollSettings.isPresent &&
                failureSettings.isPresent && reenqueueSettings.isPresent
            ) {
                val queueSettings: QueueSettings = QueueSettings.builder()
                    .withProcessingSettings(processingSettings.get())
                    .withPollSettings(pollSettings.get())
                    .withFailureSettings(failureSettings.get())
                    .withReenqueueSettings(reenqueueSettings.get())
                    .withExtSettings(parseExtSettings(settings)).build()
                queueConfigs.add(QueueConfig(queueLocation.get(), queueSettings))
            }
        }
        checkErrors()
        return queueConfigs
    }


    private fun readRawSettings(filePath: Path?): MutableMap<String, String> {
        try {
            Files.newInputStream(filePath).use { `is` ->
                val props = Properties()
                props.load(`is`)
                val map: MutableMap<String, String?> = LinkedHashMap()
                for (name in props.stringPropertyNames()) {
                    map[name] = props.getProperty(name)
                }
                return cleanupProperties(map)
            }
        } catch (ioe: IOException) {
            throw IllegalArgumentException("cannot read queue properties: file=$filePath", ioe)
        }
    }

    private fun splitRawSettingsByQueueId(rawSettings: Map<String, String>): Map<String, MutableMap<String, String>> {
        val result: MutableMap<String, MutableMap<String, String>> = LinkedHashMap()
        val settingPattern = Pattern.compile("$settingsPrefix\\.([A-Za-z0-9\\-_]+)\\.(.*)")
        rawSettings.forEach { (setting: String?, value: String) ->
            val matcher = settingPattern.matcher(setting)
            if (!matcher.matches()) {
                errorMessages.add(String.format("invalid format for setting name: setting=%s", setting))
            } else {
                val queueId = matcher.group(1)
                val settingName = matcher.group(2)
                result.computeIfAbsent(queueId) { s: String? -> LinkedHashMap() }
                result[queueId]!![settingName] = value
            }
        }
        validateSettings(result)
        checkErrors()
        return result
    }

    private fun checkErrors() {
        require(errorMessages.isEmpty()) {
            "Cannot parse queue settings:" + System.lineSeparator() +
                    errorMessages.stream().sorted()
                        .collect(Collectors.joining(System.lineSeparator()))
        }
    }

    private fun cleanupProperties(rawProperties: Map<String, String?>): MutableMap<String, String> {
        return rawProperties.entries.stream()
            .filter { entry: Map.Entry<String, String?> -> entry.key.startsWith(settingsPrefix) }
            .filter { entry: Map.Entry<String, String?> ->
                entry.value != null && !entry.value!!.trim { it <= ' ' }
                    .isEmpty()
            }
            .collect(
                Collectors.toMap(
                    Function { entry: Map.Entry<String, String?> -> entry.key.trim { it <= ' ' } },
                    Function { entry: Map.Entry<String, String?> -> entry.value!!.trim { it <= ' ' } })
            )
    }

    private fun validateSettings(queuesSettings: Map<String, MutableMap<String, String>>) {
        queuesSettings.forEach { (queueId: String?, settings: Map<String, String>) ->
            for (setting in settings.keys) {
                if (!ALLOWED_SETTINGS.contains(setting) && !setting.startsWith(SETTING_ADDITIONAL + ".")) {
                    errorMessages.add(
                        String.format("%s setting is unknown: queueId=%s", setting, queueId)
                    )
                }
            }
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(QueueConfigsReader::class.java)

        /**
         * Representation of [FailRetryType.GEOMETRIC_BACKOFF]
         */
        const val VALUE_TASK_RETRY_TYPE_GEOMETRIC: String = "geometric"

        /**
         * Representation of [FailRetryType.ARITHMETIC_BACKOFF]
         */
        const val VALUE_TASK_RETRY_TYPE_ARITHMETIC: String = "arithmetic"

        /**
         * Representation of [FailRetryType.LINEAR_BACKOFF]
         */
        const val VALUE_TASK_RETRY_TYPE_LINEAR: String = "linear"

        /**
         * Representation of [ReenqueueRetryType.MANUAL]
         */
        const val VALUE_REENQUEUE_RETRY_TYPE_MANUAL: String = "manual"

        /**
         * Representation of [ReenqueueRetryType.FIXED]
         */
        const val VALUE_REENQUEUE_RETRY_TYPE_FIXED: String = "fixed"

        /**
         * Representation of [ReenqueueRetryType.SEQUENTIAL]
         */
        const val VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL: String = "sequential"

        /**
         * Representation of [ReenqueueRetryType.ARITHMETIC]
         */
        const val VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC: String = "arithmetic"

        /**
         * Representation of [ReenqueueRetryType.GEOMETRIC]
         */
        const val VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC: String = "geometric"

        /**
         * Representation of [ProcessingMode.USE_EXTERNAL_EXECUTOR]
         */
        const val VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR: String = "use-external-executor"

        /**
         * Representation of [ProcessingMode.WRAP_IN_TRANSACTION]
         */
        const val VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION: String = "wrap-in-transaction"

        /**
         * Representation of [ProcessingMode.SEPARATE_TRANSACTIONS]
         */
        const val VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS: String = "separate-transactions"

        /**
         * Representation of [ProcessingSettings.getProcessingMode]
         */
        const val SETTING_PROCESSING_MODE: String = "processing-mode"

        /**
         * Representation of [FailureSettings.getRetryType]
         */
        const val SETTING_RETRY_TYPE: String = "retry-type"

        /**
         * Representation of [FailureSettings.getRetryInterval]
         */
        const val SETTING_RETRY_INTERVAL: String = "retry-interval"

        /**
         * Representation of [ReenqueueSettings.getRetryType]
         */
        const val SETTING_REENQUEUE_RETRY_TYPE: String = "reenqueue-retry-type"

        /**
         * Representation of [ReenqueueSettings.getSequentialPlanOrThrow]
         */
        const val SETTING_REENQUEUE_RETRY_PLAN: String = "reenqueue-retry-plan"

        /**
         * Representation of [ReenqueueSettings.getFixedDelayOrThrow]
         */
        const val SETTING_REENQUEUE_RETRY_DELAY: String = "reenqueue-retry-delay"

        /**
         * Representation of [ReenqueueSettings.getInitialDelayOrThrow]
         */
        const val SETTING_REENQUEUE_RETRY_INITIAL_DELAY: String = "reenqueue-retry-initial-delay"

        /**
         * Representation of [ReenqueueSettings.getArithmeticStepOrThrow]
         */
        const val SETTING_REENQUEUE_RETRY_STEP: String = "reenqueue-retry-step"

        /**
         * Representation of [ReenqueueSettings.getGeometricRatioOrThrow]
         */
        const val SETTING_REENQUEUE_RETRY_RATIO: String = "reenqueue-retry-ratio"

        /**
         * Representation of [ProcessingSettings.getThreadCount]
         */
        const val SETTING_THREAD_COUNT: String = "thread-count"

        /**
         * Representation of [PollSettings.getFatalCrashTimeout]
         */
        const val SETTING_FATAL_CRASH_TIMEOUT: String = "fatal-crash-timeout"

        /**
         * Representation of [PollSettings.getBetweenTaskTimeout]
         */
        const val SETTING_BETWEEN_TASK_TIMEOUT: String = "between-task-timeout"

        /**
         * Representation of [PollSettings.getNoTaskTimeout]
         */
        const val SETTING_NO_TASK_TIMEOUT: String = "no-task-timeout"

        /**
         * Representation of [QueueLocation.getTableName]
         */
        const val SETTING_TABLE: String = "table"

        /**
         * Representation of [QueueLocation.getIdSequence]
         */
        const val SETTING_ID_SEQUENCE: String = "id-sequence"

        /**
         * Representation of [QueueSettings.getExtSettings]
         */
        const val SETTING_ADDITIONAL: String = "additional-settings"

        private val ALLOWED_SETTINGS: Set<String> = HashSet(
            Arrays.asList(
                SETTING_PROCESSING_MODE, SETTING_BETWEEN_TASK_TIMEOUT, SETTING_TABLE,
                SETTING_NO_TASK_TIMEOUT, SETTING_ID_SEQUENCE, SETTING_FATAL_CRASH_TIMEOUT,
                SETTING_REENQUEUE_RETRY_DELAY, SETTING_REENQUEUE_RETRY_PLAN, SETTING_REENQUEUE_RETRY_INITIAL_DELAY,
                SETTING_REENQUEUE_RETRY_RATIO, SETTING_REENQUEUE_RETRY_TYPE, SETTING_REENQUEUE_RETRY_STEP,
                SETTING_RETRY_TYPE, SETTING_RETRY_INTERVAL, SETTING_THREAD_COUNT, SETTING_THREAD_COUNT
            )
        )

        private fun parseExtSettings(settings: Map<String, String>): ExtSettings {
            val extSettingsPrefix = SETTING_ADDITIONAL + '.'
            val map: MutableMap<String, String> = LinkedHashMap()
            for ((key, value) in settings) {
                if (key.startsWith(extSettingsPrefix)) {
                    map[key.substring(extSettingsPrefix.length)] = value
                }
            }
            return ExtSettings.Companion.builder().withSettings(map).build()
        }


        private fun overrideExistingSettings(
            existingSettings: MutableMap<String, String>,
            newSettings: Map<String, String>
        ) {
            newSettings.forEach { (key: String, newValue: String) ->
                val existingValue = existingSettings[key]
                if (existingValue != null) {
                    log.info(
                        "overriding queue property: name={}, existingValue={}, newValue={}",
                        key, existingValue, newValue
                    )
                }
                existingSettings[key] = newValue
            }
        }
    }
}

package io.github.jopenlibs.dbkqueue.settings

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.*
import java.util.stream.Collectors

/**
 * @author Oleg Kandaurov
 * @since 22.08.2017
 */
class QueueConfigsReaderTest {
    fun write(vararg lines: String?): Path {
        val tempFile = Files.createTempFile(tempDir, "queue", ".properties")
        Files.write(tempFile, Arrays.asList(*lines), StandardOpenOption.WRITE, StandardOpenOption.APPEND)
        tempFile.toFile().deleteOnExit()
        return tempFile
    }


    private fun createReader(path: Path): QueueConfigsReader {
        return createReader(Arrays.asList(path))
    }

    private fun createReader(paths: List<Path>): QueueConfigsReader {
        return QueueConfigsReader(paths, "q",
            {
                ProcessingSettings.builder()
                    .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).withThreadCount(1)
            },
            {
                PollSettings.builder()
                    .withBetweenTaskTimeout(Duration.ofSeconds(9))
                    .withNoTaskTimeout(Duration.ofSeconds(99))
                    .withFatalCrashTimeout(Duration.ofSeconds(999))
            },
            {
                FailureSettings.builder()
                    .withRetryInterval(Duration.ofMinutes(9)).withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
            },
            {
                ReenqueueSettings.builder()
                    .withRetryType(ReenqueueRetryType.MANUAL)
            })
    }

    private fun createConfig(tableName: String, queueId: String, settings: QueueSettings): QueueConfig {
        return QueueConfig(
            QueueLocation.builder().withTableName(tableName)
                .withQueueId(QueueId(queueId)).build(), settings
        )
    }

    @Test
    fun should_read_simple_config() {
        val path = write(
            "q.testQueue.table=foo",
            "q.testQueue.between-task-timeout=PT0.1S",
            "q.testQueue.no-task-timeout=PT5S"
        )
        val queueConfigsReader = createReader(path)
        val configs = queueConfigsReader.parse()

        assertThat(configs[0].location).isEqualTo(
            QueueLocation.builder()
                .withQueueId(QueueId("testQueue")).withTableName("foo").build()
        )
        assertThat(configs[0].settings.pollSettings).isEqualTo(
            PollSettings.builder()
                .withBetweenTaskTimeout(Duration.ofMillis(100L))
                .withNoTaskTimeout(Duration.ofSeconds(5L))
                .withFatalCrashTimeout(Duration.ofSeconds(999))
                .build()
        )
    }

    @Test
    fun should_read_simple_config_with_id_sequence() {
        val path = write(
            "q.testQueue.table=foo",
            "q.testQueue.id-sequence=sequence"
        )
        val queueConfigsReader = createReader(path)
        val configs = queueConfigsReader.parse()
        assertThat(configs[0].location).isEqualTo(
            QueueLocation.builder().withTableName("foo")
                .withQueueId(QueueId("testQueue")).withIdSequence("sequence").build()
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_read_full_config() {
        val path = write(
            "q.testQueue.table=foo",
            "q.testQueue.between-task-timeout=PT0.1S",
            "q.testQueue.no-task-timeout=PT5S",
            "q.testQueue.fatal-crash-timeout=PT1H",
            "q.testQueue.thread-count=3",
            "q.testQueue.retry-type=linear",
            "q.testQueue.retry-interval=PT30S",
            "q.testQueue.reenqueue-retry-type=fixed",
            "q.testQueue.reenqueue-retry-plan=PT1S,PT2S",
            "q.testQueue.reenqueue-retry-delay=PT5S",
            "q.testQueue.reenqueue-retry-initial-delay=PT1M",
            "q.testQueue.reenqueue-retry-ratio=1",
            "q.testQueue.reenqueue-retry-step=PT2S",
            "q.testQueue.processing-mode=use-external-executor",
            "q.testQueue.additional-settings.custom=val1"
        )
        val queueConfigsReader = QueueConfigsReader(Arrays.asList(path), "q")
        val configs: Collection<QueueConfig> = queueConfigsReader.parse()
        assertThat(configs).isEqualTo(
            listOf(
                createConfig(
                    "foo", "testQueue",
                    QueueSettings.builder()
                        .withProcessingSettings(
                            ProcessingSettings.builder()
                                .withThreadCount(3)
                                .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                                .build()
                        )
                        .withPollSettings(
                            PollSettings.builder()
                                .withBetweenTaskTimeout(Duration.ofMillis(100L))
                                .withNoTaskTimeout(Duration.ofSeconds(5L))
                                .withFatalCrashTimeout(Duration.ofHours(1))
                                .build()
                        )
                        .withFailureSettings(
                            FailureSettings.builder()
                                .withRetryType(FailRetryType.LINEAR_BACKOFF)
                                .withRetryInterval(Duration.ofSeconds(30))
                                .build()
                        )
                        .withReenqueueSettings(
                            ReenqueueSettings.builder()
                                .withRetryType(ReenqueueRetryType.FIXED)
                                .withFixedDelay(Duration.ofSeconds(5))
                                .withInitialDelay(Duration.ofMinutes(1))
                                .withGeometricRatio(1L)
                                .withArithmeticStep(Duration.ofSeconds(2))
                                .withSequentialPlan(Arrays.asList(Duration.ofSeconds(1), Duration.ofSeconds(2)))
                                .build()
                        )
                        .withExtSettings(
                            ExtSettings.builder().withSettings(object : LinkedHashMap<String, String>() {
                                init {
                                    put("custom", "val1")
                                }
                            }).build()
                        )
                        .build()
                )
            )
        )
    }

    @Test
    fun should_not_parse_properties_in_bad_format() {
        val path = write(
            "q.testQueue*.processing-mode=unknown-mode"
        )
        val queueConfigsReader = createReader(path)

        val err = assertThrows<IllegalArgumentException> {
            queueConfigsReader.parse()
        }
        assertThat(err.message).isEqualTo(
            "Cannot parse queue settings:" + System.lineSeparator() +
                    "invalid format for setting name: setting=q.testQueue*.processing-mode"
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_fail_when_parse_errors() {
        val path = write(
            "q.testQueue.table=foo",
            "q.testQueue.between-task-timeout=between-task",
            "q.testQueue.no-task-timeout=no-task",
            "q.testQueue.fatal-crash-timeout=fatal-crash",
            "q.testQueue.thread-count=count",
            "q.testQueue.retry-type=unknown-retry-type",
            "q.testQueue.retry-interval=retry-interval",
            "q.testQueue.processing-mode=unknown-mode1",
            "q.testQueue.processing-mode=unknown-mode2"
        )
        val queueConfigsReader = createReader(path)

        val err = assertThrows<IllegalArgumentException> {
            queueConfigsReader.parse()
        }
        assertThat(err.message).isEqualTo(
            "Cannot parse queue settings:" + System.lineSeparator() +
                    "cannot parse setting: name=between-task-timeout, value=between-task, exception=DateTimeParseException(Text cannot be parsed to a Duration)" + System.lineSeparator() +
                    "cannot parse setting: name=fatal-crash-timeout, value=fatal-crash, exception=DateTimeParseException(Text cannot be parsed to a Duration)" + System.lineSeparator() +
                    "cannot parse setting: name=no-task-timeout, value=no-task, exception=DateTimeParseException(Text cannot be parsed to a Duration)" + System.lineSeparator() +
                    "cannot parse setting: name=processing-mode, value=unknown-mode2, exception=IllegalArgumentException(unknown processing mode: name=unknown-mode2)" + System.lineSeparator() +
                    "cannot parse setting: name=retry-interval, value=retry-interval, exception=DateTimeParseException(Text cannot be parsed to a Duration)" + System.lineSeparator() +
                    "cannot parse setting: name=retry-type, value=unknown-retry-type, exception=IllegalArgumentException(unknown retry type: name=unknown-retry-type)" + System.lineSeparator() +
                    "cannot parse setting: name=thread-count, value=count, exception=NumberFormatException(For input string: \"count\")"
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_fail_when_unknown_settings() {
        val path = write(
            "q.testQueue.table=foo",
            "q.testQueue.unknown1=unknown-val"
        )

        val queueConfigsReader = QueueConfigsReader(listOf(path), "q")
        val err = assertThrows<IllegalArgumentException> {
            queueConfigsReader.parse()
        }
        assertThat(err.message).isEqualTo(
            "Cannot parse queue settings:" + System.lineSeparator() +
                    "unknown1 setting is unknown: queueId=testQueue"
        )
    }

    @Test
    fun should_fail_when_build_errors() {
        val path = write(
            "q.testQueue.table=foo",
            "q.testQueue.between-task-timeout=PT5S",
            "q.testQueue.thread-count=1",
            "q.testQueue.retry-type=geometric"
        )

        val queueConfigsReader = QueueConfigsReader(listOf(path), "q")
        val err = assertThrows<IllegalArgumentException> {
            queueConfigsReader.parse()
        }
        assertThat(err.message).isEqualTo(
            "Cannot parse queue settings:" + System.lineSeparator() +
                    "cannot build failure settings: queueId=testQueue, msg=null" + System.lineSeparator() +
                    "cannot build poll settings: queueId=testQueue, msg=null" + System.lineSeparator() +
                    "cannot build processing settings: queueId=testQueue, msg=null" + System.lineSeparator() +
                    "cannot build reenqueue settings: queueId=testQueue, msg=null"
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_read_and_override_simple_config() {
        val basePath = write(
            "q.testQueue.table=foo",
            "q.testQueue.between-task-timeout=PT0.1S",
            "q.testQueue.no-task-timeout=PT5S"
        )
        val overridePath = write(
            "q.testQueue.table=bar",
            "q.testQueue.fatal-crash-timeout=PT2S",
            "q.testQueue.between-task-timeout=PT1S",
            "q.testQueue.no-task-timeout=PT10S"
        )
        val queueConfigsReader = createReader(listOf(basePath, overridePath))
        val configs = queueConfigsReader.parse()
        assertThat(configs[0].settings.pollSettings).isEqualTo(
            PollSettings.builder()
                .withBetweenTaskTimeout(Duration.ofSeconds(1L))
                .withNoTaskTimeout(Duration.ofSeconds(10L))
                .withFatalCrashTimeout(Duration.ofSeconds(2L)).build()
        )
    }

    @Test
    @Throws(Exception::class)
    fun should_check_file_existence() {
        val path = Paths.get("invalid")
        val err = assertThrows<IllegalArgumentException> {
            createReader(path)
        }
        assertThat(err.message).isEqualTo("config path must be a file: files=[invalid]")
    }

    @Test
    @Throws(Exception::class)
    fun should_parse_retry_types() {
        val path = write(
            "q.testQueue1.table=foo",
            "q.testQueue1.between-task-timeout=PT0S",
            "q.testQueue1.no-task-timeout=PT0S",
            "q.testQueue1.retry-type=geometric",

            "q.testQueue2.table=foo",
            "q.testQueue2.between-task-timeout=PT0S",
            "q.testQueue2.no-task-timeout=PT0S",
            "q.testQueue2.retry-type=arithmetic",

            "q.testQueue3.table=foo",
            "q.testQueue3.between-task-timeout=PT0S",
            "q.testQueue3.no-task-timeout=PT0S",
            "q.testQueue3.retry-type=linear"
        )
        val queueConfigsReader = createReader(path)
        val configs: Collection<QueueConfig> = queueConfigsReader.parse()
        assertThat(
            configs.stream().collect(
                Collectors.toMap(
                    { config: QueueConfig -> config.location.queueId.asString() },
                    { config: QueueConfig -> config.settings.failureSettings.retryType })
            )
        ).isEqualTo(
            object : LinkedHashMap<String, FailRetryType>() {
                init {
                    put("testQueue1", FailRetryType.GEOMETRIC_BACKOFF)
                    put("testQueue2", FailRetryType.ARITHMETIC_BACKOFF)
                    put("testQueue3", FailRetryType.LINEAR_BACKOFF)
                }
            }
        )
    }

    @Test
    fun should_parse_reenqueue_retry_types() {
        val path = write( // без явного указания reenqueue-retry-type
            "q.testQueue0.table=foo",
            "q.testQueue0.between-task-timeout=PT0S",
            "q.testQueue0.no-task-timeout=PT0S",  // с manual стратегией

            "q.testQueue1.table=foo",
            "q.testQueue1.between-task-timeout=PT0S",
            "q.testQueue1.no-task-timeout=PT0S",
            "q.testQueue1.reenqueue-retry-type=manual",  // с фиксированной задержкой в 10 секунд

            "q.testQueue2.table=foo",
            "q.testQueue2.between-task-timeout=PT0S",
            "q.testQueue2.no-task-timeout=PT0S",
            "q.testQueue2.reenqueue-retry-type=fixed",
            "q.testQueue2.reenqueue-retry-delay=PT10S",  // с планом для переоткладывания на 1с, 2с, 3с, 3с, 3с, ...

            "q.testQueue3.table=foo",
            "q.testQueue3.between-task-timeout=PT0S",
            "q.testQueue3.no-task-timeout=PT0S",
            "q.testQueue3.reenqueue-retry-type=sequential",
            "q.testQueue3.reenqueue-retry-plan=PT1S,PT2S,PT3S",  // с арифметической прогрессией - первый член = 10с, шаг = 1с

            "q.testQueue5.table=foo",
            "q.testQueue5.between-task-timeout=PT0S",
            "q.testQueue5.no-task-timeout=PT0S",
            "q.testQueue5.reenqueue-retry-type=arithmetic",
            "q.testQueue5.reenqueue-retry-initial-delay=PT10S",
            "q.testQueue5.reenqueue-retry-step=PT1S",  // с геометрической прогрессией - первый член = 10с, знаменатель = 3

            "q.testQueue7.table=foo",
            "q.testQueue7.between-task-timeout=PT0S",
            "q.testQueue7.no-task-timeout=PT0S",
            "q.testQueue7.reenqueue-retry-type=geometric",
            "q.testQueue7.reenqueue-retry-initial-delay=PT10S",
            "q.testQueue7.reenqueue-retry-ratio=3"
        )
        val queueConfigsReader = createReader(path)
        val configs: Collection<QueueConfig> = queueConfigsReader.parse()
        assertThat(
            configs.stream().collect(
                Collectors.toMap(
                    { config: QueueConfig -> config.location.queueId.asString() },
                    { config: QueueConfig -> config.settings.reenqueueSettings })
            )
        ).isEqualTo(
            object :
                LinkedHashMap<String, ReenqueueSettings>() {
                init {
                    put(
                        "testQueue0", ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL)
                            .build()
                    )
                    put(
                        "testQueue1", ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL)
                            .build()
                    )
                    put(
                        "testQueue2", ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED)
                            .withFixedDelay(Duration.ofSeconds(10L))
                            .build()
                    )
                    put(
                        "testQueue3", ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.SEQUENTIAL)
                            .withSequentialPlan(
                                listOf(
                                    Duration.ofSeconds(1L),
                                    Duration.ofSeconds(2L),
                                    Duration.ofSeconds(3L)
                                )
                            )
                            .build()
                    )
                    put(
                        "testQueue5", ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC)
                            .withInitialDelay(Duration.ofSeconds(10L))
                            .withArithmeticStep(Duration.ofSeconds(1L))
                            .build()
                    )
                    put(
                        "testQueue7", ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC)
                            .withInitialDelay(Duration.ofSeconds(10L))
                            .withGeometricRatio(3L)
                            .build()
                    )
                }
            })
    }

    @Test
    @Throws(Exception::class)
    fun should_parse_processing_modes() {
        val path = write(
            "q.testQueue1.table=foo",
            "q.testQueue1.between-task-timeout=PT0S",
            "q.testQueue1.no-task-timeout=PT0S",
            "q.testQueue1.processing-mode=separate-transactions",

            "q.testQueue2.table=foo",
            "q.testQueue2.between-task-timeout=PT0S",
            "q.testQueue2.no-task-timeout=PT0S",
            "q.testQueue2.processing-mode=wrap-in-transaction",

            "q.testQueue3.table=foo",
            "q.testQueue3.between-task-timeout=PT0S",
            "q.testQueue3.no-task-timeout=PT0S",
            "q.testQueue3.processing-mode=use-external-executor"
        )
        val queueConfigsReader = createReader(path)
        val configs: Collection<QueueConfig> = queueConfigsReader.parse()
        assertThat(
            configs.stream().collect(
                Collectors.toMap(
                    { config: QueueConfig -> config.location.queueId.asString() },
                    { config: QueueConfig -> config.settings.processingSettings.processingMode })
            )
        ).isEqualTo(
            object : LinkedHashMap<String, ProcessingMode>() {
                init {
                    put("testQueue1", ProcessingMode.SEPARATE_TRANSACTIONS)
                    put("testQueue2", ProcessingMode.WRAP_IN_TRANSACTION)
                    put("testQueue3", ProcessingMode.USE_EXTERNAL_EXECUTOR)
                }
            }
        )
    }

    companion object {
        private lateinit var tempDir: Path

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun beforeClass() {
            tempDir = Files.createTempDirectory(
                Paths.get(System.getProperty("user.dir"), "build"),
                QueueConfigsReaderTest::class.java.simpleName
            )
            tempDir.toFile().deleteOnExit()
        }
    }
}
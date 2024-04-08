package io.github.jopenlibs.dbkqueue.settings

import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Mockito
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import ru.yoomoney.tech.dbqueue.config.QueueService
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.*

// TODO: test must be refactored: use await instead of sleep,
//  and guarantee start of QueueConfigsReloader threads before modify config
class QueueConfigsReloaderTest {

    @Test
    @Throws(Exception::class)
    fun should_not_reload_bad_configs() {
        val configPath = write("")

        val queueService = Mockito.mock(QueueService::class.java)
        val reader = Mockito.spy(createReader(listOf(configPath)))

        val reloader = QueueConfigsReloader(reader, queueService)
        reloader.start()
        Thread.sleep(1000)
        Files.write(configPath, "q.testname.table=foo".toByteArray(StandardCharsets.UTF_8))
        Thread.sleep(1000)
        reloader.stop()
        verify(reader, atLeastOnce()).parse()
        verifyNoInteractions(queueService)
    }

    @Test
    @Throws(Exception::class)
    fun should_reload_correct_configs() {
        val configPath = write("")

        val queueService: QueueService = mock()
        val reader = spy(createReader(listOf(configPath)))

        val reloader = QueueConfigsReloader(reader, queueService)
        reloader.start()
        Thread.sleep(1000)
        Files.write(
            configPath, ("q.testname.table=foo" + System.lineSeparator() + "q.testname.thread-count=1")
                .toByteArray(StandardCharsets.UTF_8)
        )
        val queueConfigs: List<QueueConfig> = reader.parse()
        Thread.sleep(1000)
        reloader.stop()
        verify(reader, atLeast(2)).parse()
        verify(queueService, atLeastOnce()).updateQueueConfigs(queueConfigs)
    }

    companion object {
        private lateinit var tempDir: Path

        @JvmStatic
        @BeforeClass
        @Throws(Exception::class)
        fun beforeClass() {
            tempDir = Files.createTempDirectory(
                Paths.get(System.getProperty("user.dir"), "build"),
                QueueConfigsReloaderTest::class.java.simpleName
            )
            tempDir.toFile().deleteOnExit()
        }
    }

    private fun write(vararg lines: String?): Path {
        val tempFile = Files.createTempFile(tempDir, "queue", ".properties")
        Files.write(tempFile, Arrays.asList(*lines), StandardOpenOption.WRITE, StandardOpenOption.APPEND)
        tempFile.toFile().deleteOnExit()
        return tempFile
    }

    private fun createReader(paths: List<Path>): QueueConfigsReader {
        return QueueConfigsReader(paths, "q",
            {
                ProcessingSettings.builder()
                    .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
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
}
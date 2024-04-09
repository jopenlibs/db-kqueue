package io.github.jopenlibs.dbkqueue.settings

import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertTrue

class FileWatcherTest {
    @Test
    fun should_watch_file_changes() {
        val tempDir = Files.createTempDirectory(
            Paths.get(System.getProperty("user.dir"), "build"),
            javaClass.simpleName
        )
        tempDir.toFile().deleteOnExit()

        val tempFile = Files.createTempFile(tempDir, "queue", ".properties")
        tempFile.toFile().deleteOnExit()

        Files.write(
            tempFile,
            "1".toByteArray(StandardCharsets.UTF_8),
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        )
        val fileHasChanged = AtomicBoolean(false)
        val fileWatcher = FileWatcher(tempFile) {
            fileHasChanged.set(true)
        }
        fileWatcher.startWatch()
        // wait for executor start
        Thread.sleep(500)
        Files.write(
            tempFile,
            "2".toByteArray(StandardCharsets.UTF_8),
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        )
        val startTime = System.currentTimeMillis()
        var elapsed: Long = 0
        while (Duration.ofMillis(elapsed).seconds < 2 && !fileHasChanged.get()) {
            elapsed = System.currentTimeMillis() - startTime
            Thread.sleep(100)
        }
        fileWatcher.stopWatch()

        assertTrue(fileHasChanged.get())
    }
}
package ru.yoomoney.tech.dbqueue.settings

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.file.ClosedWatchServiceException
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Provides tracking changes in a target file
 *
 * @author Oleg Kandaurov
 * @since 12.10.2021
 */
internal class FileWatcher(
    private val watchedFile: Path,
    private val onChangeCallback: Runnable
) {
    private val executor: ExecutorService = Executors.newSingleThreadExecutor()

    private val watchedDir: Path = watchedFile.parent

    private var watchServiceFileDir: WatchService? = null

    /**
     * Constructor
     *
     * @param watchedFile      file to watch
     * @param onChangeCallback callback invoked on file change
     */
    init {
        require(watchedFile.toFile().isFile) { "watched file is not a file: file=$watchedFile" }
    }

    /**
     * Start track file changes
     */
    @Synchronized
    fun startWatch() {
        log.info("Starting watch for file changes: file={}", watchedFile)
        try {
            startWatchFileDirectory()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    /**
     * Stop track file changes
     */
    @Synchronized
    fun stopWatch() {
        try {
            log.info("Stopping watch for file changes: file={}", watchedFile)
            if (watchServiceFileDir != null) {
                watchServiceFileDir!!.close()
            }
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

    @Synchronized
    @Throws(IOException::class)
    private fun startWatchFileDirectory() {
        if (watchServiceFileDir != null) {
            watchServiceFileDir!!.close()
        }
        watchServiceFileDir = watchedDir!!.fileSystem.newWatchService()
        watchedDir.register(
            watchServiceFileDir,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE,
            StandardWatchEventKinds.ENTRY_MODIFY
        )
        executor.execute { doWatch(watchServiceFileDir, watchedFile!!.toFile(), onChangeCallback) }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(FileWatcher::class.java)

        private fun doWatch(watchService: WatchService?, file: File, callback: Runnable) {
            try {
                var watchKey: WatchKey
                while ((watchService!!.take().also { watchKey = it }) != null) {
                    val polledEvents = watchKey.pollEvents()
                    val fileModified = polledEvents
                        .stream()
                        .filter { watchEvent: WatchEvent<*> -> watchEvent.kind() != StandardWatchEventKinds.OVERFLOW }
                        .map { watchEvent: WatchEvent<*> -> watchEvent.context().toString() }
                        .anyMatch { fileName: String -> fileName == file.name }
                    if (fileModified) {
                        callback.run()
                    }
                    watchKey.reset()
                }
            } catch (exc: InterruptedException) {
                Thread.currentThread().interrupt()
            } catch (ignored: ClosedWatchServiceException) {
            }
        }
    }
}

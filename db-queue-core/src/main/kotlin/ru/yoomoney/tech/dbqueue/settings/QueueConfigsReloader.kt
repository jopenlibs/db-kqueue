package ru.yoomoney.tech.dbqueue.settings

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.yoomoney.tech.dbqueue.config.QueueService
import java.nio.file.Path
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors

/**
 * Dynamic reload of queue configuration.
 *
 *
 * Reloads queue configuration if source files has been changed.
 *
 * @author Oleg Kandaurov
 * @since 12.10.2021
 */
class QueueConfigsReloader(
    private val queueConfigsReader: QueueConfigsReader,
    private val queueService: QueueService
) {
    private val fileWatchers: List<FileWatcher>

    /**
     * Constructor
     *
     * @param queueConfigsReader queue configuration parser
     * @param queueService       queue service
     */
    init {
        this.fileWatchers = queueConfigsReader.getConfigPaths().stream()
            .map { path: Path -> FileWatcher(path, Runnable { this.reload() }) }
            .collect(Collectors.toList())
    }

    @Synchronized
    private fun reload() {
        try {
            val queueConfigs = queueConfigsReader.parse()
            val diff = queueService.updateQueueConfigs(queueConfigs)
            log.info("queue configuration updated: diff={}", diff)
        } catch (exc: RuntimeException) {
            log.error("cannot reload queue configs", exc)
        }
    }

    /**
     * Starts automatic reload of queue configuration
     */
    @Synchronized
    fun start() {
        fileWatchers.forEach(Consumer { obj: FileWatcher -> obj.startWatch() })
    }

    /**
     * Stops automatic reload of queue configuration
     */
    @Synchronized
    fun stop() {
        fileWatchers.forEach(Consumer { obj: FileWatcher -> obj.stopWatch() })
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(QueueConfigsReloader::class.java)
    }
}

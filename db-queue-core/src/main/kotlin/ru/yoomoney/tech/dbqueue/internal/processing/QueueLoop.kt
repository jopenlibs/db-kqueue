package ru.yoomoney.tech.dbqueue.internal.processing

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.concurrent.Volatile

/**
 * Вспомогательный класс, для задания стратегии
 * выполнения задачи в цикле
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
interface QueueLoop {
    /**
     * Запустить выполнение кода
     *
     * @param runnable код для исполнения
     */
    suspend fun doRun(runnable: suspend () -> Unit)

    /**
     * Продолжить исполнение кода
     */
    fun doContinue()

    /**
     * Приостановить исполнение кода
     *
     * @param timeout       промежуток на который следует приостановить работу
     * @param waitInterrupt признак, что разрешено прервать ожидание и продолжить работу
     */
    fun doWait(timeout: Duration?, waitInterrupt: WaitInterrupt)

    /**
     * Получить признак, что исполнение кода приостановлено
     *
     * @return true, если исполнение приостановлено
     */
    val isPaused: Boolean

    /**
     * Безусловно приостановить исполнение кода
     */
    fun pause()

    /**
     * Безусловно продолжить исполнение кода
     */
    fun unpause()

    /**
     * Cтратегия выполнения задачи в потоке
     */
    class WakeupQueueLoop : QueueLoop {
        private val monitor = Any()

        @Volatile
        private var isWakedUp = false

        @Volatile
        override var isPaused: Boolean = true
            private set

        override suspend fun doRun(runnable: suspend () -> Unit) {
            while (!Thread.currentThread().isInterrupted) {
                try {
                    synchronized(monitor) {
                        while (isPaused) {
                            (monitor as Object).wait()
                        }
                    }
                    runnable()
                } catch (ignored: InterruptedException) {
                    log.info("sleep interrupted: threadName={}", Thread.currentThread().name)
                    Thread.currentThread().interrupt()
                }
            }
        }

        override fun doContinue() {
            synchronized(monitor) {
                isWakedUp = true
                (monitor as Object).notify()
            }
        }

        override fun doWait(timeout: Duration?, waitInterrupt: WaitInterrupt) {
            try {
                synchronized(monitor) {
                    val plannedWakeupTime = System.currentTimeMillis() + timeout!!.toMillis()
                    var timeToSleep = plannedWakeupTime - System.currentTimeMillis()
                    while (timeToSleep > 1L) {
                        if (!isWakedUp) {
                            (monitor as Object).wait(timeToSleep)
                        }
                        if (isWakedUp && waitInterrupt == WaitInterrupt.ALLOW) {
                            break
                        }
                        if (isWakedUp && waitInterrupt == WaitInterrupt.DENY) {
                            isWakedUp = false
                        }
                        timeToSleep = plannedWakeupTime - System.currentTimeMillis()
                    }
                    isWakedUp = false
                }
            } catch (ignored: InterruptedException) {
                log.info("sleep interrupted: threadName={}", Thread.currentThread().name)
                Thread.currentThread().interrupt()
            }
        }

        override fun pause() {
            isPaused = true
        }

        override fun unpause() {
            synchronized(monitor) {
                isPaused = false
                (monitor as Object).notifyAll()
            }
        }

        companion object {
            private val log: Logger = LoggerFactory.getLogger(QueueLoop::class.java)
        }
    }

    /**
     * Признак прерывания ожидания
     */
    enum class WaitInterrupt {
        /**
         * Прерывание разрешено
         */
        ALLOW,

        /**
         * Прерывание запрещено
         */
        DENY
    }
}

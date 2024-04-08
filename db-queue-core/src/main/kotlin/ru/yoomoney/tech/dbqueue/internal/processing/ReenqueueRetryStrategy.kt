package ru.yoomoney.tech.dbqueue.internal.processing

import ru.yoomoney.tech.dbqueue.api.TaskRecord
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings
import java.time.Duration
import java.util.*
import kotlin.math.pow

/**
 * Стратегия по вычислению задержки перед следующим выполнением задачи в случае, если задачу требуется вернуть в очередь.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
interface ReenqueueRetryStrategy {
    /**
     * Вычисляет задержку перед следующим выполнением задачи.
     *
     * @param taskRecord информация о задаче
     * @return задержка
     */
    fun calculateDelay(taskRecord: TaskRecord): Duration?

    /**
     * Фабрика для создания стратегии повтора задача, в зависимости от настроек
     */
    object Factory {
        /**
         * Создает стратегию на основе переданных настроек переоткладывания задач для очереди.
         *
         * @param reenqueueSettings настройки переоткладывания задач
         * @return стратегия
         */
        fun create(reenqueueSettings: ReenqueueSettings): ReenqueueRetryStrategy {

            return when (reenqueueSettings.retryType) {
                ReenqueueRetryType.MANUAL -> ManualReenqueueRetryStrategy()
                ReenqueueRetryType.FIXED -> FixedDelayReenqueueRetryStrategy(reenqueueSettings.fixedDelayOrThrow)
                ReenqueueRetryType.SEQUENTIAL -> SequentialReenqueueRetryStrategy(reenqueueSettings.sequentialPlanOrThrow)
                ReenqueueRetryType.ARITHMETIC -> ArithmeticReenqueueRetryStrategy(
                    reenqueueSettings.initialDelayOrThrow,
                    reenqueueSettings.arithmeticStepOrThrow
                )

                ReenqueueRetryType.GEOMETRIC -> GeometricReenqueueRetryStrategy(
                    reenqueueSettings.initialDelayOrThrow,
                    reenqueueSettings.geometricRatioOrThrow
                )

                else -> throw IllegalArgumentException("unknown re-enqueue retry type: type=" + reenqueueSettings.retryType)
            }
        }
    }


    /**
     * Стратегия, которая не вычисляет задержку. Используется в случае, если продолжительность задержки выбирается
     * пользователем для каждого выполнения задачи отдельно.
     */
    class ManualReenqueueRetryStrategy : ReenqueueRetryStrategy {
        override fun calculateDelay(taskRecord: TaskRecord): Duration {
            throw UnsupportedOperationException(
                "re-enqueue delay must be set explicitly via 'reenqueue(Duration)' method call"
            )
        }
    }

    /**
     * Стратегия, которая возвращает фиксированную задержку для любого выполнения задачи.
     */
    class FixedDelayReenqueueRetryStrategy internal constructor(private val delay: Duration) : ReenqueueRetryStrategy {
        override fun calculateDelay(taskRecord: TaskRecord): Duration {
            return delay
        }
    }

    /**
     * Стратегия, которая возвращает задержку на основании некоторой конечной последовательности.
     * Если количество попыток выполнения задачи превышает размер последовательности, будет возвращен ее последний
     * элемент.
     */
    class SequentialReenqueueRetryStrategy internal constructor(retryPlan: List<Duration>) :
        ReenqueueRetryStrategy {
        private val retryPlan: List<Duration> = Collections.unmodifiableList(retryPlan)

        override fun calculateDelay(taskRecord: TaskRecord): Duration {
            if (taskRecord.reenqueueAttemptsCount >= retryPlan.size) {
                return retryPlan[retryPlan.size - 1]
            }
            return retryPlan[taskRecord.reenqueueAttemptsCount.toInt()]
        }
    }

    /**
     * Стратегия, которая возвращает задержку на основании арифметической прогрессии, заданной с помощью ее
     * первого члена и разности.
     */
    class ArithmeticReenqueueRetryStrategy internal constructor(
        private val initialDelay: Duration,
        private val step: Duration
    ) : ReenqueueRetryStrategy {
        override fun calculateDelay(taskRecord: TaskRecord): Duration {
            return initialDelay!!.plus(step!!.multipliedBy(taskRecord.reenqueueAttemptsCount))
        }
    }

    /**
     * Стратегия, которая возвращает задержку на основании геометрической прогрессии, заданной с помощью ее первого
     * члена и целочисленного знаменателя.
     */
    class GeometricReenqueueRetryStrategy internal constructor(
        private val initialDelay: Duration,
        private val ratio: Long
    ) : ReenqueueRetryStrategy {
        override fun calculateDelay(taskRecord: TaskRecord): Duration {
            return initialDelay.multipliedBy(
                ratio.toDouble().pow(taskRecord.reenqueueAttemptsCount.toDouble()).toLong()
            )
        }
    }
}

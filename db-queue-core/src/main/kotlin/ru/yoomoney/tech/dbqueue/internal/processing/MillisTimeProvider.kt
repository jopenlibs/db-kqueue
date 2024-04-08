package ru.yoomoney.tech.dbqueue.internal.processing

/**
 * Поставщик текущего времени в миллисекундах.
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
interface MillisTimeProvider {
    /**
     * Получить время в миллисекундах.
     *
     * @return время в миллисекундах
     */
    val millis: Long

    /**
     * Поставщик системного времени
     */
    class SystemMillisTimeProvider : MillisTimeProvider {
        override val millis: Long
            get() = System.currentTimeMillis()
    }
}

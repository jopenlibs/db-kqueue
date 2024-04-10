package io.github.jopenlibs.dbkqueue.internal.processing

/**
 * Тип результата обработки задачи в очереди
 *
 * @author Oleg Kandaurov
 * @since 27.08.2017
 */
enum class QueueProcessingStatus {
    /**
     * Задача была обрабатана
     */
    PROCESSED,

    /**
     * Задача не была найдена и обработка не состоялась
     */
    SKIPPED
}

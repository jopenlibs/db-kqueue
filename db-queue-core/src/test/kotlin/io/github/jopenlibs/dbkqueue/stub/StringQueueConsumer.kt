package io.github.jopenlibs.dbkqueue.stub

import ru.yoomoney.tech.dbqueue.api.QueueConsumer
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer
import ru.yoomoney.tech.dbqueue.settings.QueueConfig

/**
 * Queue consumer without payload transformation
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
abstract class StringQueueConsumer(override val queueConfig: QueueConfig) : QueueConsumer<String> {
    override val payloadTransformer: TaskPayloadTransformer<String>
        get() = NoopPayloadTransformer.instance
}

package io.github.jopenlibs.dbkqueue.stub

import io.github.jopenlibs.dbkqueue.api.QueueConsumer
import io.github.jopenlibs.dbkqueue.api.TaskPayloadTransformer
import io.github.jopenlibs.dbkqueue.api.impl.NoopPayloadTransformer
import io.github.jopenlibs.dbkqueue.settings.QueueConfig

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

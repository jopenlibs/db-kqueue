package ru.yoomoney.tech.dbqueue.api.impl

import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer

/**
 * Default payload transformer, which performs no transformation
 * and returns the same string as in the raw payload.
 *
 *
 * Use where no transformation required.
 */
class NoopPayloadTransformer private constructor() : TaskPayloadTransformer<String> {
    override fun toObject(payload: String?): String? {
        return payload
    }

    override fun fromObject(payload: String?): String? {
        return payload
    }

    companion object {
        /**
         * Get payload transformer instance.
         *
         * @return Singleton of transformer.
         */
        val instance: NoopPayloadTransformer = NoopPayloadTransformer()
    }
}

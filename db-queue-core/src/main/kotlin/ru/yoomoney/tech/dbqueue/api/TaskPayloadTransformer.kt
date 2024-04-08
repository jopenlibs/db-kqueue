package ru.yoomoney.tech.dbqueue.api

/**
 * Marshaller and unmarshaller for the payload in the task
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 10.07.2017
</PayloadT> */
interface TaskPayloadTransformer<PayloadT> {
    /**
     * Unmarshall the string payload from the task into the object with task data
     *
     * @param payload task payload
     * @return Object with task data
     */
    fun toObject(payload: String?): PayloadT?

    /**
     * Marshall the typed object with task parameters into string payload.
     *
     * @param payload task payload
     * @return string with the task payload.
     */
    fun fromObject(payload: PayloadT?): String?
}

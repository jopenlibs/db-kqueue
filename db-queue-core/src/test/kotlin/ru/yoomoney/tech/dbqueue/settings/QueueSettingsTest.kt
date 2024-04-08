package ru.yoomoney.tech.dbqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.Test

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
class QueueSettingsTest {
    @Test
    @Throws(Exception::class)
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(QueueSettings::class.java).verify()
    }
}
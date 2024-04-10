package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.assertj.core.api.Assertions.assertThat
import java.util.*
import kotlin.test.Test

class ProcessingSettingsTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ProcessingSettings::class.java)
            .withIgnoredFields("observers")
            .suppress(Warning.NONFINAL_FIELDS)
            .usingGetClass().verify()
    }

    @Test
    fun should_set_value() {
        val oldValue = ProcessingSettings.builder()
            .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).withThreadCount(1).build()
        val newValue = ProcessingSettings.builder()
            .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).withThreadCount(0).build()
        val diff = oldValue.setValue(newValue)
        assertThat(diff).isEqualTo(Optional.of("processingSettings(threadCount=0<1,processingMode=SEPARATE_TRANSACTIONS<USE_EXTERNAL_EXECUTOR)"))
        assertThat(oldValue).isEqualTo(newValue)
    }
}
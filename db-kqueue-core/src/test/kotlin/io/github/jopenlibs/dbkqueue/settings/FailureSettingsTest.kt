package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.assertj.core.api.Assertions.assertThat
import java.time.Duration
import java.util.*
import kotlin.test.Test

class FailureSettingsTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(FailureSettings::class.java)
            .withIgnoredFields("observers")
            .suppress(Warning.NONFINAL_FIELDS)
            .usingGetClass().verify()
    }

    @Test
    fun should_set_value() {
        val oldValue = FailureSettings.builder()
            .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
            .withRetryInterval(Duration.ofSeconds(1)).build()
        val newValue = FailureSettings.builder()
            .withRetryType(FailRetryType.ARITHMETIC_BACKOFF)
            .withRetryInterval(Duration.ofSeconds(5)).build()
        val diff = oldValue.setValue(newValue)

        assertThat(diff).isEqualTo(Optional.of("failureSettings(retryType=ARITHMETIC_BACKOFF<GEOMETRIC_BACKOFF,retryInterval=PT5S<PT1S)"))
        assertThat(oldValue).isEqualTo(newValue)
    }
}
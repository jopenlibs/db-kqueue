package ru.yoomoney.tech.dbqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import java.time.Duration
import java.util.*

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
        Assert.assertThat(
            diff,
            CoreMatchers.equalTo(Optional.of("failureSettings(retryType=ARITHMETIC_BACKOFF<GEOMETRIC_BACKOFF,retryInterval=PT5S<PT1S)"))
        )
        Assert.assertThat(oldValue, CoreMatchers.equalTo(newValue))
    }
}
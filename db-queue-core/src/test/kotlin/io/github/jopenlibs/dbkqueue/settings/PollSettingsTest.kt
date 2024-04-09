package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class PollSettingsTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(PollSettings::class.java)
            .withIgnoredFields("observers")
            .suppress(Warning.NONFINAL_FIELDS)
            .usingGetClass().verify()
    }

    @Test
    fun should_set_value() {
        val oldValue = PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(1))
            .withNoTaskTimeout(Duration.ofSeconds(2)).withFatalCrashTimeout(Duration.ofSeconds(3)).build()
        val newValue = PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
            .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6)).build()
        val diff = oldValue.setValue(newValue)

        assertThat(diff).isEqualTo(Optional.of("pollSettings(betweenTaskTimeout=PT4S<PT1S,noTaskTimeout=PT5S<PT2S,fatalCrashTimeout=PT6S<PT3S)"))
        assertThat(oldValue).isEqualTo(newValue)
    }
}
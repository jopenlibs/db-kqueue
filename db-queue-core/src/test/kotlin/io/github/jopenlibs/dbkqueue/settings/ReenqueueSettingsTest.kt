package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.*

class ReenqueueSettingsTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ReenqueueSettings::class.java)
            .withIgnoredFields("observers")
            .suppress(Warning.NONFINAL_FIELDS)
            .usingGetClass().verify()
    }

    @Test
    fun should_set_value_arithmetic_to_fixed() {
        val oldValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.ARITHMETIC)
            .withArithmeticStep(Duration.ofSeconds(1))
            .withInitialDelay(Duration.ofSeconds(2)).build()
        val newValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.FIXED).withFixedDelay(Duration.ofSeconds(3)).build()
        val diff = oldValue.setValue(newValue)
        assertThat(diff).isEqualTo(
            Optional.of("reenqueueSettings(type=FIXED<ARITHMETIC,arithmeticStep=null<PT1S,initialDelay=null<PT2S,fixedDelay=PT3S<null)")
        )
        assertThat(oldValue).isEqualTo(newValue)
    }

    @Test
    fun should_set_value_geometric_to_sequential() {
        val oldValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.GEOMETRIC)
            .withGeometricRatio(2L)
            .withInitialDelay(Duration.ofSeconds(2)).build()
        val newValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.SEQUENTIAL)
            .withSequentialPlan(Arrays.asList(Duration.ofSeconds(1), Duration.ofSeconds(2))).build()
        val diff = oldValue.setValue(newValue)
        assertThat(diff).isEqualTo(
            Optional.of("reenqueueSettings(type=SEQUENTIAL<GEOMETRIC,geometricRatio=null<2,initialDelay=null<PT2S,sequentialPlan=[PT1S, PT2S]<null)")
        )
        assertThat(oldValue).isEqualTo(newValue)
    }

    @Test
    fun should_set_value_fixed_to_manual() {
        val oldValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.FIXED)
            .withFixedDelay(Duration.ofSeconds(2)).build()
        val newValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.MANUAL).build()
        val diff = oldValue.setValue(newValue)
        assertThat(diff).isEqualTo(
            Optional.of("reenqueueSettings(type=MANUAL<FIXED,fixedDelay=null<PT2S)")
        )
        assertThat(oldValue).isEqualTo(newValue)
    }

    @Test
    fun should_throw_exception_for_arithmetic_when_arithmetic_step_not_defined() {
        val err = assertThrows<IllegalArgumentException> {
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC)
                .withInitialDelay(Duration.ofMinutes(1))
                .build()
        }
        assertThat(err.message).isEqualTo("arithmeticStep and initialDelay must not be empty when retryType=ARITHMETIC")
    }

    @Test
    fun should_throw_exception_for_arithmetic_when_initial_delay_not_defined() {
        val err = assertThrows<IllegalArgumentException> {
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC)
                .withArithmeticStep(Duration.ofMinutes(1)).build()
        }
        assertThat(err.message).isEqualTo("arithmeticStep and initialDelay must not be empty when retryType=ARITHMETIC")
    }

    @Test
    fun should_throw_exception_for_geometric_when_ratio_not_defined() {
        val err = assertThrows<IllegalArgumentException> {
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC)
                .withInitialDelay(Duration.ofMinutes(1))
                .build()
        }
        assertThat(err.message).isEqualTo("geometricRatio and initialDelay must not be empty when retryType=GEOMETRIC")
    }

    @Test
    fun should_throw_exception_for_geometric_when_initial_delay_not_defined() {
        val err = assertThrows<IllegalArgumentException> {
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC).withGeometricRatio(1L).build()
        }
        assertThat(err.message).isEqualTo("geometricRatio and initialDelay must not be empty when retryType=GEOMETRIC")
    }

    @Test
    fun should_throw_exception_when_fixed_delay_not_set() {
        val err = assertThrows<IllegalArgumentException> {
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED).build()
        }
        assertThat(err.message).isEqualTo("fixedDelay must not be empty when retryType=FIXED")
    }

    @Test
    fun should_throw_exception_when_sequential_plan_not_set() {
        val err = assertThrows<IllegalArgumentException> {
            ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.SEQUENTIAL)
                .build()
        }
        assertThat(err.message).isEqualTo("sequentialPlan must not be empty when retryType=SEQUENTIAL")
    }
}

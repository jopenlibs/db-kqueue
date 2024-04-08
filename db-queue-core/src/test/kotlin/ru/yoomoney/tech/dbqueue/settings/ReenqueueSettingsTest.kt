package ru.yoomoney.tech.dbqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import java.time.Duration
import java.util.*

class ReenqueueSettingsTest {
    @JvmField
    @Rule
    var thrown: ExpectedException = ExpectedException.none()

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
        Assert.assertThat(
            diff,
            CoreMatchers.equalTo(Optional.of("reenqueueSettings(type=FIXED<ARITHMETIC,arithmeticStep=null<PT1S,initialDelay=null<PT2S,fixedDelay=PT3S<null)"))
        )
        Assert.assertThat(oldValue, CoreMatchers.equalTo(newValue))
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
        Assert.assertThat(
            diff,
            CoreMatchers.equalTo(Optional.of("reenqueueSettings(type=SEQUENTIAL<GEOMETRIC,geometricRatio=null<2,initialDelay=null<PT2S,sequentialPlan=[PT1S, PT2S]<null)"))
        )
        Assert.assertThat(oldValue, CoreMatchers.equalTo(newValue))
    }

    @Test
    fun should_set_value_fixed_to_manual() {
        val oldValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.FIXED)
            .withFixedDelay(Duration.ofSeconds(2)).build()
        val newValue = ReenqueueSettings.builder()
            .withRetryType(ReenqueueRetryType.MANUAL).build()
        val diff = oldValue.setValue(newValue)
        Assert.assertThat(
            diff,
            CoreMatchers.equalTo(Optional.of("reenqueueSettings(type=MANUAL<FIXED,fixedDelay=null<PT2S)"))
        )
        Assert.assertThat(oldValue, CoreMatchers.equalTo(newValue))
    }

    @Test
    fun should_throw_exception_for_arithmetic_when_arithmetic_step_not_defined() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("arithmeticStep and initialDelay must not be empty when retryType=ARITHMETIC")
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC).withInitialDelay(Duration.ofMinutes(1))
            .build()
    }

    @Test
    fun should_throw_exception_for_arithmetic_when_initial_delay_not_defined() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("arithmeticStep and initialDelay must not be empty when retryType=ARITHMETIC")
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC)
            .withArithmeticStep(Duration.ofMinutes(1)).build()
    }

    @Test
    fun should_throw_exception_for_geometric_when_ratio_not_defined() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("geometricRatio and initialDelay must not be empty when retryType=GEOMETRIC")
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC).withInitialDelay(Duration.ofMinutes(1))
            .build()
    }

    @Test
    fun should_throw_exception_for_geometric_when_initial_delay_not_defined() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("geometricRatio and initialDelay must not be empty when retryType=GEOMETRIC")
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC).withGeometricRatio(1L).build()
    }

    @Test
    fun should_throw_exception_when_fixed_delay_not_set() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("fixedDelay must not be empty when retryType=FIXED")
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED).build()
    }

    @Test
    fun should_throw_exception_when_sequential_plan_not_set() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("sequentialPlan must not be empty when retryType=SEQUENTIAL")
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.SEQUENTIAL)
            .build()
    }
}

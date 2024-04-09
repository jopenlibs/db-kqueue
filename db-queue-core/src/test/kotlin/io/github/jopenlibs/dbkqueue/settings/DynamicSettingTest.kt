package io.github.jopenlibs.dbkqueue.settings

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiFunction
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DynamicSettingTest {
    @Test
    fun should_not_invoke_observer_when_no_changes() {
        val observerInvoked = AtomicBoolean(false)
        val oldSetting = SimpleDynamicSetting("old")
        oldSetting.registerObserver { oldVal: SimpleDynamicSetting?, newVal: SimpleDynamicSetting? ->
            observerInvoked.set(true)
        }
        val newSetting = SimpleDynamicSetting("old")
        val diff = oldSetting.setValue(newSetting)

        assertThat(diff).isEqualTo(Optional.empty<Any>())
        assertFalse(observerInvoked.get())
        assertThat(oldSetting).isEqualTo(newSetting)
    }

    @Test
    fun should_invoke_observer_when_setting_changed() {
        val observerInvoked = AtomicBoolean(false)
        val oldSetting = SimpleDynamicSetting("old")
        oldSetting.registerObserver { oldVal: SimpleDynamicSetting?, newVal: SimpleDynamicSetting? ->
            observerInvoked.set(true)
        }
        val newSetting = SimpleDynamicSetting("new")
        val diff = oldSetting.setValue(newSetting)

        assertThat(diff).isEqualTo(Optional.of("new<old"))
        assertTrue(observerInvoked.get())
        assertThat(oldSetting).isEqualTo(newSetting)
    }

    @Test
    fun should_not_update_setting_when_observer_fails() {
        val oldSetting = SimpleDynamicSetting("old")
        oldSetting.registerObserver { oldVal: SimpleDynamicSetting?, newVal: SimpleDynamicSetting? ->
            throw RuntimeException("exc")
        }
        val newSetting = SimpleDynamicSetting("new")
        val diff = oldSetting.setValue(newSetting)

        assertThat(diff).isEqualTo(Optional.empty<Any>())
        assertThat(oldSetting).isNotEqualTo(newSetting)
    }

    class SimpleDynamicSetting(private var text: String) : DynamicSetting<SimpleDynamicSetting>() {
        override val name: String
            get() = "simple"

        override val diffEvaluator: BiFunction<SimpleDynamicSetting, SimpleDynamicSetting, String>
            get() = BiFunction { oldVal: SimpleDynamicSetting, newVal: SimpleDynamicSetting
                ->
                newVal.text + '<' + oldVal.text
            }


        override fun copyFields(newValue: SimpleDynamicSetting) {
            this.text = newValue.text
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || javaClass != other.javaClass) return false
            val that = other as SimpleDynamicSetting
            return text == that.text
        }

        override fun hashCode(): Int {
            return Objects.hash(text)
        }
    }
}
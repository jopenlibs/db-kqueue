package ru.yoomoney.tech.dbqueue.settings

import org.hamcrest.CoreMatchers
import org.hamcrest.MatcherAssert
import org.junit.Test
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiFunction

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
        MatcherAssert.assertThat(diff, CoreMatchers.equalTo(Optional.empty<Any>()))
        MatcherAssert.assertThat(observerInvoked.get(), CoreMatchers.equalTo(false))
        MatcherAssert.assertThat(oldSetting, CoreMatchers.equalTo(newSetting))
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
        MatcherAssert.assertThat(diff, CoreMatchers.equalTo(Optional.of("new<old")))
        MatcherAssert.assertThat(observerInvoked.get(), CoreMatchers.equalTo(true))
        MatcherAssert.assertThat(oldSetting, CoreMatchers.equalTo(newSetting))
    }

    @Test
    fun should_not_update_setting_when_observer_fails() {
        val oldSetting = SimpleDynamicSetting("old")
        oldSetting.registerObserver { oldVal: SimpleDynamicSetting?, newVal: SimpleDynamicSetting? ->
            throw RuntimeException("exc")
        }
        val newSetting = SimpleDynamicSetting("new")
        val diff = oldSetting.setValue(newSetting)
        MatcherAssert.assertThat(diff, CoreMatchers.equalTo(Optional.empty<Any>()))
        MatcherAssert.assertThat(oldSetting, CoreMatchers.not(CoreMatchers.equalTo(newSetting)))
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

        override fun equals(o: Any?): Boolean {
            if (this === o) return true
            if (o == null || javaClass != o.javaClass) return false
            val that = o as SimpleDynamicSetting
            return text == that.text
        }

        override fun hashCode(): Int {
            return Objects.hash(text)
        }
    }
}
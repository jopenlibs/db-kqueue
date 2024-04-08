package io.github.jopenlibs.dbkqueue.settings

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.hamcrest.CoreMatchers
import org.junit.Assert
import org.junit.Test
import java.util.*

class ExtSettingsTest {
    @Test
    fun should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ExtSettings::class.java)
            .withIgnoredFields("observers")
            .suppress(Warning.NONFINAL_FIELDS)
            .usingGetClass().verify()
    }

    @Test
    fun should_set_value() {
        val oldMap: MutableMap<String, String> = LinkedHashMap()
        oldMap["same"] = "1"
        oldMap["old"] = "0"
        val newMap: MutableMap<String, String> = LinkedHashMap()
        newMap["same"] = "2"
        newMap["new"] = "3"
        val oldValue = ExtSettings.builder().withSettings(oldMap).build()
        val newValue = ExtSettings.builder().withSettings(newMap).build()
        val diff = oldValue.setValue(newValue)
        Assert.assertThat(diff, CoreMatchers.equalTo(Optional.of("extSettings(same=2<1,new=3<null,old=null<0)")))
        Assert.assertThat(oldValue, CoreMatchers.equalTo(newValue))
    }
}
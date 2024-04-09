package io.github.jopenlibs.dbkqueue

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOptions
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition
import org.junit.Before
import org.junit.Test
import java.util.*
import java.util.stream.Collectors

/**
 * @author Oleg Kandaurov
 * @since 03.08.2017
 */
class ArchitectureTest {
    private var classes: JavaClasses? = null

    @Before
    fun importClasses() {
        classes = ClassFileImporter(ImportOptions())
            .importPackages(BASE_PACKAGE)
    }

    @Test
    fun test2() {
        val rule = ArchRuleDefinition.classes().that().resideInAnyPackage(
            *fullNames("api")
        )
            .should().accessClassesThat().resideInAnyPackage(*fullNames("api..", "settings.."))
            .orShould().accessClassesThat().resideInAnyPackage("java..")
            .because("api must not depend on implementation details")
        rule.check(classes)
    }

    @Test
    fun test3() {
        val rule = ArchRuleDefinition.classes().that().resideInAnyPackage(
            *fullNames("settings")
        )
            .should().accessClassesThat().resideInAnyPackage(*fullNames("settings"))
            .orShould().accessClassesThat().resideInAnyPackage("java..")
            .because("settings must not depend on implementation details")
        rule.check(classes)
    }

    @Test
    fun test4() {
        val rule = ArchRuleDefinition.noClasses().that().resideInAnyPackage(
            *fullNames("settings..", "api..", "dao..", "spring..")
        )
            .should().accessClassesThat().resideInAnyPackage(*fullNames("internal.."))
            .because("public classes must not depend on internal details")
        rule.check(classes)
    }


    companion object {
        private const val BASE_PACKAGE = "io.github.jopenlibs.dbkqueue"

        private fun fullNames(vararg relativeName: String): Array<String> {
            return Arrays.stream(relativeName).map { name: String -> BASE_PACKAGE + "." + name }
                .collect(Collectors.toList()).toTypedArray<String>()
        }
    }
}

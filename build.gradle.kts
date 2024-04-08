plugins {
    kotlin("jvm") version "1.9.22"
}

val version: String by project
val group = "io.github.jopenlibs"

repositories {
    mavenCentral()
}

dependencies {

    constraints {
        api("org.slf4j:slf4j-api:1.7.36")
        api("com.google.code.findbugs:jsr305:3.0.1")
        api("com.google.code.findbugs:annotations:3.0.1")
        api("org.junit.jupiter:junit-jupiter-api:5.10.2")
        api("org.junit.vintage:junit-vintage-engine:5.8.1")
        api("org.apache.logging.log4j:log4j-core:2.17.1")
        api("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
        api("org.mockito.kotlin:mockito-kotlin:5.2.1")
        api("com.tngtech.archunit:archunit:0.9.1")
        api("nl.jqno.equalsverifier:equalsverifier:3.9")
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    }
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
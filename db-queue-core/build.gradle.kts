plugins {
    kotlin("jvm") version "1.9.22"
}

group = "io.github.jopenlibs"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.junit.vintage:junit-vintage-engine:5.8.1")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testImplementation("ch.qos.logback:logback-classic:1.5.3")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.2.1")

    // TODO: review dependencies
    testImplementation("com.tngtech.archunit:archunit:0.9.1")
    testImplementation("nl.jqno.equalsverifier:equalsverifier:3.9")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}
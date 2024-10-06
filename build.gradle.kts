
plugins {
    kotlin("jvm") version "1.8.21"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://nexus.co-workerhou.se/repository/maven-public")
    }
    mavenLocal()
}

dependencies {
    implementation("org.apache.flink:flink-java:1.20.0")
    implementation("org.apache.flink:flink-streaming-java:1.20.0")
    implementation("org.apache.flink:flink-clients:1.20.0")

    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("org.apache.logging.log4j:log4j-api:2.20.0")
    implementation("org.apache.logging.log4j:log4j-core:2.20.0")

    testImplementation("org.apache.flink:flink-test-utils:1.20.0")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens", "java.base/java.time=ALL-UNNAMED")
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}

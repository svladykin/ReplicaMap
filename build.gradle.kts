import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    `java-library`
    maven
}

group = "com.vladykin"
version = "0.4-SNAPSHOT"

repositories {
    mavenCentral()
}

val kafkaVersion = "2.3.1"

dependencies {
    implementation("org.apache.kafka", "kafka-clients", kafkaVersion)

    testImplementation("org.junit.jupiter", "junit-jupiter", "5.4.1")
    testImplementation("com.salesforce.kafka.test", "kafka-junit5", "3.2.1")

    testRuntimeOnly("org.apache.kafka", "kafka_2.12", kafkaVersion)
    testRuntimeOnly("org.slf4j", "slf4j-simple", "1.7.26")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    val jar by getting(Jar::class) {
        manifest {
            attributes["Automatic-Module-Name"] = "com.vladykin.replicamap"
        }
    }

    val sourcesJar by creating(Jar::class) {
        dependsOn(JavaPlugin.CLASSES_TASK_NAME)
        classifier = "sources"
        from(sourceSets["main"].allSource)
    }

    val javadocJar by creating(Jar::class) {
        dependsOn(JavaPlugin.JAVADOC_TASK_NAME)
        classifier = "javadoc"
        from(tasks["javadoc"])
    }

    artifacts {
        add("archives", sourcesJar)
        add("archives", javadocJar)
        add("archives", jar)
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    minHeapSize = "1G"
    maxHeapSize = "1G"
    testLogging {
        events(STARTED, PASSED, FAILED, SKIPPED, STANDARD_OUT, STANDARD_ERROR)
        exceptionFormat = FULL
    }
    filter {
//        includeTest(
//                "com.vladykin.replicamap.kafka.KReplicaMapManagerMultithreadedWindowTest",
//                "testMultithreadedSlidingWindowWithRestart"
//        )
//        includeTest(
//                "com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleShardingTest",
//                "testSimpleSharding"
//        )
    }
}
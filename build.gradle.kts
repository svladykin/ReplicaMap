import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    `java-library`
    `maven-publish`
}

group = "com.vladykin"
version = "0.5.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val kafkaVersion = if (!project.hasProperty("kafkaVersion")) "1.0.0"
                   else project.property("kafkaVersion") as String

println("Using kafkaVersion: $kafkaVersion")

dependencies {
    implementation("org.apache.kafka", "kafka-clients", kafkaVersion)

    testImplementation("org.junit.jupiter", "junit-jupiter", "5.4.1")
    testImplementation("com.salesforce.kafka.test", "kafka-junit5", "3.2.1")

    testRuntimeOnly("org.apache.kafka", "kafka_2.12", kafkaVersion) {
        exclude("org.slf4j")
    }
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
        archiveClassifier.set("sources")
        from(sourceSets["main"].allSource)
    }

    val javadocJar by creating(Jar::class) {
        dependsOn(JavaPlugin.JAVADOC_TASK_NAME)
        archiveClassifier.set("javadoc")
        from(javadoc.get().destinationDir)
    }

    artifacts {
        add("archives", sourcesJar)
        add("archives", javadocJar)
        add("archives", jar)
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "replicamap"
            from(components["java"])
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])
            pom {
                name.set("ReplicaMap Library")
                description.set("Embedded key-value database for Java applications replicated over Kafka")
                url.set("https://github.com/svladykin/ReplicaMap")
                licenses {
                    license {
                        name.set("MIT")
                    }
                }
                developers {
                    developer {
                        id.set("svladykin")
                        name.set("Sergi Vladykin")
                        email.set("sergi.vladykin@gmail.com")
                    }
                }
                scm {
                    url.set("https://github.com/svladykin/ReplicaMap")
                }
            }
        }
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    minHeapSize = "2G"
    maxHeapSize = "4G"
    testLogging {
        events(STARTED, PASSED, FAILED, SKIPPED, STANDARD_OUT, STANDARD_ERROR)
        exceptionFormat = FULL
    }
//    filter {
//        includeTest(
//                "com.vladykin.replicamap.kafka.KReplicaMapManagerMultithreadedWindowTest",
//                "testMultithreadedSlidingWindowWithRestart"
//        )
//        includeTest(
//                "com.vladykin.replicamap.kafka.KReplicaMapManagerSimpleShardingTest",
//                "testSimpleSharding"
//        )
//    }
}
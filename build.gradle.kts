plugins {
    `java-library`
    maven
}

group = "com.vladykin"
version = "0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka", "kafka-clients", "2.3.0")

    testImplementation("org.junit.jupiter", "junit-jupiter", "5.4.1")
    testImplementation("com.salesforce.kafka.test", "kafka-junit5", "3.1.1")

    testRuntimeOnly("org.apache.kafka", "kafka_2.12", "2.3.0")
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
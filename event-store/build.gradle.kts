plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Need this to fetch confluent's kafka-clients dependency
  // isAllowInsecureProtocol is needed for gradle 8
  maven {
    url = uri("http://packages.confluent.io/maven")
    isAllowInsecureProtocol = true
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("com.typesafe:config:1.3.2")
  implementation(platform("org.hypertrace.core.kafkastreams.framework:kafka-bom:0.4.7"))
  implementation("org.apache.kafka:kafka-clients")
  implementation("commons-io:commons-io:2.14.0")
  implementation("org.slf4j:slf4j-api:1.7.30")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}

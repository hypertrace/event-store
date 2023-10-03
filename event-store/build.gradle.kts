plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Need this to fetch confluent's kafka-clients dependency
  maven("http://packages.confluent.io/maven")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("com.typesafe:config:1.3.2")
  implementation(platform("org.hypertrace.core.kafkastreams.framework:kafka-bom:0.3.8"))
  implementation("org.apache.kafka:kafka-clients")
  implementation("commons-io:commons-io:2.7")
  implementation("org.slf4j:slf4j-api:1.7.30")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}

package org.hypertrace.core.eventstore;

import com.typesafe.config.Config;

public class EventConsumerConfig {
  private Config sourceConfig;

  private String storeType; // Kafka, File, HDFS, BigQuery etc

  public EventConsumerConfig(String storeType, Config config) {
    this.storeType = storeType;
    this.sourceConfig = config;
  }

  /**
   * Consumer Config specific to the source
   *
   * @return
   */
  public Config getSourceConfig() {
    return sourceConfig;
  }

  public String getStoreType() {
    return storeType;
  }
}

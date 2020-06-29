package org.hypertrace.core.eventstore;

import com.typesafe.config.Config;

/**
 * Simple wrapper for now.
 */
public class EventProducerConfig {

  private Config config;

  private String storeType; // Kafka, File, HDFS etc


  public EventProducerConfig(String storeType, Config config) {
    this.storeType = storeType;
    this.config = config;
  }

  /**
   * Config specific to the sink which will be used to instantiate
   *
   * @return
   */
  public Config getConfig() {
    return config;
  }

  /**
   * @return
   */
  public String getType() {
    return storeType;
  }

}

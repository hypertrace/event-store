package org.hypertrace.core.eventstore;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class EventStoreConfig {

  private static final String CONSUMER_CONFIG_PREFIX = "consumer";
  private static final String PRODUCER_CONFIG_PREFIX = "producer";
  private final Config config;

  public EventStoreConfig(Config config) {
    this.config = config;
  }

  public Config getStoreConfig() {
    return this.config;
  }

  public Config getConsumerConfig() {
    if (this.config.hasPath(CONSUMER_CONFIG_PREFIX)) {
      return this.config.getConfig(CONSUMER_CONFIG_PREFIX);
    }
    return ConfigFactory.empty();
  }

  public Config getProducerConfig() {
    if (this.config.hasPath(PRODUCER_CONFIG_PREFIX)) {
      return this.config.getConfig(PRODUCER_CONFIG_PREFIX);
    }
    return ConfigFactory.empty();
  }
}

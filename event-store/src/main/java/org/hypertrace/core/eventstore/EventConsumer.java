package org.hypertrace.core.eventstore;

import java.time.Duration;

/**
 * While this is similar to Kafka consumer. The key difference is that the consumer is initialized
 * per topic unlike Kafka where one can consume events across all topics.
 */
public interface EventConsumer<T> {

  /**
   * Initializes the event consumer
   */
  boolean init(EventConsumerConfig config);

  /**
   * Gets the next batch of events
   */
  T[] poll();

  /**
   * Gets the next batch of events with maxWaitTime.
   */
  T[] poll(Duration maxWaitTime);

  /**
   * Returns the current state of consumer. Can include the offset for each partition, time stamp,
   * stat's etc. This state can be saved and used to resume the consumer
   */
  ConsumerState getState();

  /**
   * Checkpoints the consumer state.
   */
  boolean checkpoint(ConsumerState consumerState);

  /**
   * Closes the consumer.
   */
  boolean close();
}

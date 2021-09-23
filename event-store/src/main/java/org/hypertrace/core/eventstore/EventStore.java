package org.hypertrace.core.eventstore;

import com.typesafe.config.Config;
import java.util.List;

/** */
public interface EventStore {
  /**
   * Initialize the event store
   *
   * @param eventstoreConfig
   * @return
   */
  boolean init(EventStoreConfig eventstoreConfig);

  /**
   * create a topic based on the params, e.g. num partitions, schema etc
   *
   * @param topicName
   * @param params
   * @return true if successful
   */
  boolean createTopic(String topicName, Config params);

  /**
   * Delete the topic
   *
   * @param topicName
   * @return true if successful, we might have to return a Future here later since delete is not
   *     trivial
   */
  boolean deleteTopic(String topicName);

  /**
   * Lists all the topic
   *
   * @return
   */
  List<String> listTopics();

  /**
   * Creates a consumer that can be used to fetch events.
   *
   * @param topicName
   * @param config
   * @return
   */
  <K extends Object, V extends Object> EventConsumer<K, V> createConsumer(
      String topicName, EventConsumerConfig config);

  /**
   * Creates a producer that can be used to send events in a streaming/batch fashion.
   *
   * @param topicName
   * @param config
   * @return
   */
  <K extends Object, V extends Object> EventProducer<K, V> createProducer(
      String topicName, EventProducerConfig config);
}

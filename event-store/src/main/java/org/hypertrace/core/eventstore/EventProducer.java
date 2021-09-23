package org.hypertrace.core.eventstore;

import java.util.List;

/** Simple interface to send events to any sink - Kafka, File, BigQuery, HDFS or any service */
public interface EventProducer<K, V> {

  /**
   * Initialize a event producer with Sink properties
   *
   * @param config
   */
  void init(EventProducerConfig config);

  /**
   * Sends data to underlying sink, async by default unless sync=true in the init configs.
   *
   * @param key key
   * @param event event
   */
  void send(K key, V event);

  /**
   * Sends data to underlying sink, async by default unless sync=true in the init configs
   *
   * @param eventsMap
   */
  void batchSend(List<KeyValuePair<K, V>> eventsMap);

  /** flush everything in local buffer */
  void flush();

  // Close this data sink.
  void close();
}

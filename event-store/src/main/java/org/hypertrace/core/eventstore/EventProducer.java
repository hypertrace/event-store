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
   * @param key message key
   * @param value message value/payload
   */
  void send(K key, V value);

  /**
   * Sends data to underlying sink, async by default unless sync=true in the init configs.
   *
   * @param key message key
   * @param value message value/payload
   * @param timestamp time stamp to be used for the event
   */
  void send(K key, V value, long timestamp);

  /**
   * Sends data to underlying sink asynchronously with a callback to handle completion or errors.
   *
   * @param key message key
   * @param value message value/payload
   * @param callback callback to be invoked when the send completes or fails
   */
  void send(K key, V value, SendCallback callback);

  /**
   * Sends data to underlying sink asynchronously with a callback to handle completion or errors.
   *
   * @param key message key
   * @param value message value/payload
   * @param timestamp time stamp to be used for the event
   * @param callback callback to be invoked when the send completes or fails
   */
  void send(K key, V value, long timestamp, SendCallback callback);

  /**
   * Sends data to underlying sink, async by default unless sync=true in the init configs
   *
   * @param events list of events
   */
  void batchSend(List<KeyValuePair<K, V>> events);

  /** flush everything in local buffer */
  void flush();

  // Close this data sink.
  void close();
}

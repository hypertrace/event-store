package org.hypertrace.core.eventstore;

import java.util.List;

/**
 * Simple interface to send events to any sink - Kafka, File, BigQuery, HDFS or any service
 */
public interface EventProducer<T> {

  /**
   * Initialize a event producer with Sink properties
   *
   * @param config
   */
  void init(EventProducerConfig config);

  /**
   * Sends data to underlying sink, async by default unless sync=true in the init configs.
   *
   * @param event event
   */
  void send(T event);


  /**
   * Sends data to underlying sink, async by default unless sync=true in the init configs
   *
   * @param eventList
   */

  void batchSend(List<T> eventList);

  /**
   * flush everything in local buffer
   */
  void flush();


  // Close this data sink.
  void close();
}

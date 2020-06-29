package org.hypertrace.core.eventstore.kafka;

import com.typesafe.config.ConfigValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventProducer<T> implements EventProducer<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventProducer.class);
  private static final String TOPIC_NAME = "topic.name";

  private Producer<byte[], T> producer;
  private String topic;

  @Override
  public void init(EventProducerConfig config) {
    Map<String, Object> producerConfig = new HashMap<>();
    Set<Entry<String, ConfigValue>> entries = config.getConfig().entrySet();
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      producerConfig.put(key, config.getConfig().getString(key));
    }
    this.topic = config.getConfig().getString(TOPIC_NAME);
    LOGGER.info("Get init configs for producer: {}",
        Arrays.toString(producerConfig.entrySet().toArray()));
    producer = new KafkaProducer<>(producerConfig);
  }

  @Override
  public void send(T buffer) {
    producer.send(new ProducerRecord<>(this.topic, buffer));

  }

  /**
   * @param buffer    message payload
   * @param timestamp record timestamp to set explicitly
   */
  public void send(T buffer, long timestamp) {
    producer.send(new ProducerRecord<>(this.topic, null, timestamp, null, buffer));
  }

  @Override
  public void batchSend(List<T> bufferList) {
    for (T buffer : bufferList) {
      send(buffer);
    }
  }

  @Override
  public void flush() {
    this.producer.flush();
  }

  @Override
  public void close() {
    this.producer.close();
  }
}

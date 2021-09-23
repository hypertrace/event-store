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
import org.hypertrace.core.eventstore.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventProducer<K, V> implements EventProducer<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventProducer.class);
  private static final String TOPIC_NAME = "topic.name";

  private Producer<K, V> producer;
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
    LOGGER.info(
        "Get init configs for producer: {}", Arrays.toString(producerConfig.entrySet().toArray()));
    producer = new KafkaProducer<>(producerConfig);
  }

  @Override
  public void send(K key, V value) {
    producer.send(new ProducerRecord<>(this.topic, key, value));
  }

  @Override
  public void send(K key, V value, long timestamp) {
    producer.send(new ProducerRecord<>(this.topic, null, timestamp, key, value));
  }

  @Override
  public void batchSend(List<KeyValuePair<K, V>> events) {
    events.forEach(entry -> this.send(entry.getKey(), entry.getValue()));
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

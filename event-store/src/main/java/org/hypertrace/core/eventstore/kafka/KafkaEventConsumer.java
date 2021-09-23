package org.hypertrace.core.eventstore.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.hypertrace.core.eventstore.ConsumerState;
import org.hypertrace.core.eventstore.EventConsumer;
import org.hypertrace.core.eventstore.EventConsumerConfig;
import org.hypertrace.core.eventstore.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of Kafka Event Consumer wrapped with Consumer in Kafka Client library. */
public class KafkaEventConsumer<K, V> implements EventConsumer<K, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventConsumer.class);
  private static final Duration DEFAULT_MAX_POLL_TIME = Duration.ofSeconds(30);
  private static final String TOPIC_NAME = "topic.name";

  private Consumer<K, V> consumer;
  private String topic;

  private static Config getDefaultKafkaConsumerConfigs() {
    Map<String, String> defaultKafkaConsumerConfigMap = new HashMap<>();
    String groupID = "KafkaEventConsumer-" + UUID.randomUUID().toString();
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
    defaultKafkaConsumerConfigMap.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    defaultKafkaConsumerConfigMap.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return ConfigFactory.parseMap(defaultKafkaConsumerConfigMap);
  }

  @Override
  public boolean init(EventConsumerConfig config) {
    Config configs = config.getSourceConfig();
    this.topic = configs.getString(TOPIC_NAME);
    Properties props =
        getKafkaConsumerConfigs(configs.withFallback(getDefaultKafkaConsumerConfigs()));
    Consumer<K, V> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    this.consumer = consumer;
    return false;
  }

  public Properties getKafkaConsumerConfigs(Config configs) {
    Map<String, String> configMap = new HashMap<>();
    Set<Entry<String, ConfigValue>> entries = configs.entrySet();
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      configMap.put(key, configs.getString(key));
    }
    Properties props = new Properties();
    props.putAll(configMap);
    return props;
  }

  @Override
  public List<KeyValuePair<K, V>> poll() {
    return this.poll(DEFAULT_MAX_POLL_TIME);
  }

  @Override
  public List<KeyValuePair<K, V>> poll(Duration maxWaitTime) {
    ConsumerRecords<K, V> records = consumer.poll(maxWaitTime);
    List<KeyValuePair<K, V>> res = new ArrayList<>(records.count());
    Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
    while (iterator.hasNext()) {
      ConsumerRecord<K, V> record = iterator.next();
      res.add(new KeyValuePair(record.key(), record.value()));
    }
    return res;
  }

  /*
   * Returns current Kafka consumer state, which contains all the topic partitions as the key, and
   * current consumed message offset.
   */
  @Override
  public ConsumerState getState() {
    Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = new HashMap<>();
    List<PartitionInfo> partitionInfos = this.consumer.partitionsFor(this.topic);
    for (PartitionInfo partitionInfo : partitionInfos) {
      TopicPartition tp = new TopicPartition(this.topic, partitionInfo.partition());
      OffsetAndMetadata om = new OffsetAndMetadata(this.consumer.position(tp));
      topicPartitionOffsetAndMetadataMap.put(tp, om);
    }
    LOGGER.info(
        "getState: {}", Arrays.toString(topicPartitionOffsetAndMetadataMap.entrySet().toArray()));
    return new KafkaConsumerState(topicPartitionOffsetAndMetadataMap);
  }

  /*
   * Checkpoint current consumer state, which is the kafka topic partition and corresponding max polled
   * record offset.
   * Please ensure all the message polled are consumed and processed then checkpoint consumer.
   */
  @Override
  public boolean checkpoint(ConsumerState consumerState) {
    if (consumerState != null) {
      Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
          ((KafkaConsumerState) consumerState).getTopicPartitionOffsetAndMetadataMap();
      LOGGER.debug(
          "checkpoint: {}",
          Arrays.toString(topicPartitionOffsetAndMetadataMap.entrySet().toArray()));
      this.consumer.commitSync(topicPartitionOffsetAndMetadataMap);
      return true;
    }
    this.consumer.commitAsync();
    return true;
  }

  @Override
  public boolean close() {
    this.consumer.close();
    return true;
  }
}

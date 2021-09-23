package org.hypertrace.core.eventstore.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hypertrace.core.eventstore.EventConsumer;
import org.hypertrace.core.eventstore.EventConsumerConfig;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreConfig;
import org.hypertrace.core.eventstore.KeyValuePair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventStoreIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaEventStoreIntegrationTest.class);

  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC_1 = "foo";
  private static final String TEST_TOPIC_2 = "bar";

  private static final String TOPIC_NAME = "topic.name";
  private static final EventStore kafkaEventStore = new KafkaEventStore();

  @BeforeAll
  public static void setup() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    EventStoreConfig eventStoreConfig = new EventStoreConfig(ConfigFactory.parseMap(configMap));
    kafkaEventStore.init(eventStoreConfig);

    // Topic Creation
    kafkaEventStore.createTopic(TEST_TOPIC_1, getCreateTopicParams(1, 1));
    kafkaEventStore.createTopic(TEST_TOPIC_2, getCreateTopicParams(1, 1));

    Thread.sleep(STABILIZE_SLEEP_DELAYS);
  }

  @AfterAll
  public static void shutDown() throws Exception {
    kafkaEventStore.deleteTopic(TEST_TOPIC_1);
    kafkaEventStore.deleteTopic(TEST_TOPIC_2);
  }

  private static Config getCreateTopicParams(Integer numPartitions, Integer replicationFactor) {
    Map<String, String> topicParamsMap = new HashMap<>();
    topicParamsMap.put("numPartitions", numPartitions.toString());
    topicParamsMap.put("replicationFactor", replicationFactor.toString());
    return ConfigFactory.parseMap(topicParamsMap);
  }

  private static EventProducerConfig getEventProducerConfig(String topic) {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(TOPIC_NAME, topic);
    configMap.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaEventProducer-" + UUID.randomUUID());
    configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    configMap.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new EventProducerConfig("kafka", ConfigFactory.parseMap(configMap));
  }

  private static EventConsumerConfig getEventConsumerConfig(String topic) {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(TOPIC_NAME, topic);
    configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new EventConsumerConfig("kafka", ConfigFactory.parseMap(configMap));
  }

  @Disabled
  public void testTopicCreationDeletion() {
    int existedTopics = kafkaEventStore.listTopics().size();
    int numTopics = 10;
    for (int i = 0; i < numTopics; i++) {
      String testTopic = "testTopicCreationDeletion-" + i;
      kafkaEventStore.createTopic(testTopic, getCreateTopicParams(1, 1));
      Assertions.assertEquals(kafkaEventStore.listTopics().size(), existedTopics + i + 1);
    }

    for (int i = 0; i < numTopics; i++) {
      String testTopic = "testTopicCreationDeletion-" + i;
      kafkaEventStore.deleteTopic(testTopic);
      Assertions.assertEquals(
          kafkaEventStore.listTopics().size(), existedTopics + numTopics - i - 1);
    }
  }

  @Disabled
  public void testSingleEventProducerConsumer() {
    // Create Producer
    EventProducer<byte[], byte[]> producer =
        kafkaEventStore.createProducer(TEST_TOPIC_1, getEventProducerConfig(TEST_TOPIC_1));
    // Create Consumer
    EventConsumer<byte[], byte[]> consumer =
        kafkaEventStore.createConsumer(TEST_TOPIC_1, getEventConsumerConfig(TEST_TOPIC_1));

    int numMsgToProduce = 100;
    List<String> producedMsg = new ArrayList<>();
    for (int i = 0; i < numMsgToProduce; i++) {
      String uuidStr = UUID.randomUUID().toString();
      producer.send(uuidStr.getBytes(), uuidStr.getBytes());
      // LOGGER.info("Produced: {} ", uuidStr);
      producedMsg.add(uuidStr);
    }
    producer.flush();

    int i = 0;
    while (i < numMsgToProduce) {
      List<KeyValuePair<byte[], byte[]>> records = consumer.poll();
      for (KeyValuePair<byte[], byte[]> record : records) {
        String key = new String(record.getKey());
        String value = new String(record.getValue());
        String expected = producedMsg.get(i++);
        LOGGER.debug("i: {}, key = {}, value = {}, expected = {}", i, key, value, expected);
        Assertions.assertEquals(key, expected);
        Assertions.assertEquals(value, expected);
      }
    }
  }

  @Disabled
  public void testBatchEventProducerConsumer() {
    // Create Producer
    EventProducer<byte[], byte[]> producer =
        kafkaEventStore.createProducer(TEST_TOPIC_2, getEventProducerConfig(TEST_TOPIC_2));
    // Create Consumer
    EventConsumer<byte[], byte[]> consumer =
        kafkaEventStore.createConsumer(TEST_TOPIC_2, getEventConsumerConfig(TEST_TOPIC_2));

    int numMsgToProduce = 10;
    int batches = 10;
    List<String> producedMsg = new ArrayList<>();
    for (int j = 0; j < batches; j++) {
      List<KeyValuePair<byte[], byte[]>> batch = new ArrayList<>();
      for (int i = 0; i < numMsgToProduce; i++) {
        String uuidStr = UUID.randomUUID().toString();
        batch.add(new KeyValuePair<>(uuidStr.getBytes(), uuidStr.getBytes()));
        producedMsg.add(uuidStr);
      }
      producer.batchSend(batch);
      producer.flush();
    }

    int i = 0;
    while (i < numMsgToProduce * batches) {
      List<KeyValuePair<byte[], byte[]>> records = consumer.poll();
      for (KeyValuePair<byte[], byte[]> record : records) {
        String key = new String(record.getKey());
        String value = new String(record.getValue());
        String expected = producedMsg.get(i++);
        LOGGER.debug("i: {}, key = {}, value = {}, expected = {}", i, key, value, expected);
        Assertions.assertEquals(key, expected);
        Assertions.assertEquals(value, expected);
      }
    }
  }
}

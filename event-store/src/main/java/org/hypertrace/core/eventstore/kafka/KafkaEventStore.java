package org.hypertrace.core.eventstore.kafka;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.hypertrace.core.eventstore.EventConsumer;
import org.hypertrace.core.eventstore.EventConsumerConfig;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** KafkaEventStore implements storage on top of Kafka. */
public class KafkaEventStore implements EventStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventStore.class);
  private static final int requestTimeout = 15000;
  private static final String TOPIC_NAME = "topic.name";
  private EventStoreConfig storeConfig;
  private AdminClient adminClient;

  @Override
  public boolean init(EventStoreConfig eventstoreConfig) {
    this.storeConfig = eventstoreConfig;
    final Map<String, Object> config = new HashMap<>();
    config.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        eventstoreConfig.getStoreConfig().getString(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    config.put(
        AdminClientConfig.CLIENT_ID_CONFIG, "KafkaEventStore-" + UUID.randomUUID().toString());

    if (eventstoreConfig.getStoreConfig().hasPath(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)) {
      config.put(
          AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
          eventstoreConfig.getStoreConfig().getString(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
    } else {
      config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
    }
    adminClient = KafkaAdminClient.create(config);
    return true;
  }

  @Override
  public boolean createTopic(String topicName, Config params) {
    int numPartitions = 1;
    if (params.hasPath("numPartitions")) {
      numPartitions = params.getInt("numPartitions");
      if (numPartitions < 0) {
        LOGGER.error("Failed to create topic: {}, numPartitions has to be larger than 0.");
        return false;
      }
    }
    short replicationFactor = 1;
    if (params.hasPath("replicationFactor")) {
      replicationFactor = (short) params.getInt("replicationFactor");
      if (replicationFactor < 0) {
        LOGGER.error("Failed to create topic: {}, replicationFactor has to be larger than 0.");
        return false;
      }
    }
    NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
    CreateTopicsResult createTopicsResult = this.adminClient.createTopics(Arrays.asList(newTopic));
    try {
      createTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to create Kafka topic: {}, Exception: {}", newTopic.toString(), e);
      return false;
    }
    return true;
  }

  @Override
  public boolean deleteTopic(String topicName) {
    DeleteTopicsResult deleteTopicsResult = this.adminClient.deleteTopics(Arrays.asList(topicName));
    try {
      deleteTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to delete Kafka topic: {}, Exception: {}", topicName, e);
      return false;
    }
    return true;
  }

  @Override
  public List<String> listTopics() {
    ListTopicsResult listTopicsResult = this.adminClient.listTopics();
    KafkaFuture<Map<String, TopicListing>> mapKafkaFuture = listTopicsResult.namesToListings();
    try {
      Map<String, TopicListing> stringTopicListingMap = mapKafkaFuture.get();
      return new ArrayList<>(stringTopicListingMap.keySet());
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to list all Kafka topics, Exception: {}", e);
      return null;
    }
  }

  @Override
  public <K extends Object, V extends Object> EventConsumer<K, V> createConsumer(
      String name, EventConsumerConfig options) {
    KafkaEventConsumer consumer = new KafkaEventConsumer();
    EventConsumerConfig consumerConfig =
        new EventConsumerConfig(
            "Kafka", this.storeConfig.getConsumerConfig().withFallback(options.getSourceConfig()));
    consumer.init(consumerConfig);
    return consumer;
  }

  @Override
  public <K extends Object, V extends Object> EventProducer<K, V> createProducer(
      String name, EventProducerConfig options) {
    KafkaEventProducer producer = new KafkaEventProducer();

    EventProducerConfig producerConfig =
        new EventProducerConfig(
            "Kafka", this.storeConfig.getProducerConfig().withFallback(options.getConfig()));
    producer.init(producerConfig);
    return producer;
  }
}

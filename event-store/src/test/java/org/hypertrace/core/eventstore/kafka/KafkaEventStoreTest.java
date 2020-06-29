package org.hypertrace.core.eventstore.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.eventstore.EventConsumer;
import org.hypertrace.core.eventstore.EventConsumerConfig;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.junit.jupiter.api.Disabled;

public class KafkaEventStoreTest {

  @Disabled
  public void testSimple() {
    String type = "Kafka";
    EventStoreProvider.register(type, KafkaEventStore.class);
    Config config = ConfigFactory.empty();

    //Initialize EventStore
    EventStore eventStore = EventStoreProvider.getEventStore(type, config);
    String topicName = "testTopic";

    //Create Producer
    EventProducerConfig producerConfig = new EventProducerConfig(type, ConfigFactory.empty());
    EventProducer<byte[]> producer = eventStore.createProducer(topicName, producerConfig);

    //Send Events
    producer.send(new byte[]{}); //single event

    List<byte[]> batchEvents = new ArrayList<byte[]>();
    producer.batchSend(batchEvents); //batch message

    //Create Consumer
    EventConsumerConfig consumerConfig = new EventConsumerConfig(type, ConfigFactory.empty());
    EventConsumer<byte[]> consumer = eventStore.createConsumer(topicName, consumerConfig);

    //Consume events
    byte[][] events = consumer.poll();


  }
}

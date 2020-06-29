package org.hypertrace.core.eventstore.kafka;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hypertrace.core.eventstore.ConsumerState;

public class KafkaConsumerState implements ConsumerState {

  private Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;

  public KafkaConsumerState(
      Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap) {

    this.topicPartitionOffsetAndMetadataMap = topicPartitionOffsetAndMetadataMap;
  }

  public Map<TopicPartition, OffsetAndMetadata> getTopicPartitionOffsetAndMetadataMap() {
    return this.topicPartitionOffsetAndMetadataMap;
  }
}

package org.hypertrace.core.eventstore;

/**
 * Represents the result of a successful send operation. This class abstracts away the underlying
 * implementation details (e.g., Kafka's RecordMetadata) and provides a common interface for
 * clients.
 */
public class SendResult {

  private final String topic;
  private final int partition;
  private final long offset;
  private final long timestamp;

  public SendResult(String topic, int partition, long offset, long timestamp) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
  }

  /** Returns the topic the record was appended to. */
  public String getTopic() {
    return topic;
  }

  /** Returns the partition the record was sent to. */
  public int getPartition() {
    return partition;
  }

  /** Returns the offset of the record in the topic/partition. */
  public long getOffset() {
    return offset;
  }

  /** Returns the timestamp of the record in the topic/partition. */
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "SendResult{"
        + "topic='"
        + topic
        + '\''
        + ", partition="
        + partition
        + ", offset="
        + offset
        + ", timestamp="
        + timestamp
        + '}';
  }
}

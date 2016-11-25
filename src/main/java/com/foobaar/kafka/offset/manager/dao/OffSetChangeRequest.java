package com.foobaar.kafka.offset.manager.dao;

import java.util.List;

public class OffSetChangeRequest {
  private String consumerGroupId;
  private String topic;
  private List<PartitionOffset> partitionOffsets;
  private String kafkaBroker;

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public void setConsumerGroupId(String consumerGroupId) {
    this.consumerGroupId = consumerGroupId;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public List<PartitionOffset> getPartitionOffsets() {
    return partitionOffsets;
  }

  public void setPartitionOffsets(List<PartitionOffset> partitionOffsets) {
    this.partitionOffsets = partitionOffsets;
  }

  public String getKafkaBroker() { return kafkaBroker; }

  public void setKafkaBroker(String kafkaBroker) { this.kafkaBroker = kafkaBroker; }
}

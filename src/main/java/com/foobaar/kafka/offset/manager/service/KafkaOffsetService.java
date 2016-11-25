package com.foobaar.kafka.offset.manager.service;

import com.foobaar.kafka.offset.manager.dao.OffSetChangeRequest;
import com.foobaar.kafka.offset.manager.dao.PartitionOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import static java.util.Collections.singleton;

@Component
public class KafkaOffsetService {
  public String changeOffset(OffSetChangeRequest req) {
    KafkaConsumer<String, String> consumer = null;
    try {
      consumer = getConsumer(req);

      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      for (PartitionOffset partitionOffset : req.getPartitionOffsets()) {
        TopicPartition topicPartition = new TopicPartition(req.getTopic(),
            partitionOffset.getPartition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
            partitionOffset.getOffset());
        offsets.put(topicPartition, offsetAndMetadata);
      }

      consumer.commitSync(offsets);
    } catch (Exception e) {
      return e.getMessage();
    } finally {
      consumer.close();
    }
    return "Offset change complete.";
  }

  public String changeOffsetToEnd(OffSetChangeRequest req, String position) {

    KafkaConsumer<String, String> consumer = null;
    try {
      consumer = getConsumer(req);
      /*
      Since we would shut down all consumers before we execute this method, rebalancing should not
      an issue as kafka-offset-handler would be the only consumer.
       */
      consumer.subscribe(singleton(req.getTopic()), new NoOpConsumerRebalanceListener());
      consumer.poll(1000);
      Collection<TopicPartition> topicPartitions = consumer.partitionsFor(req.getTopic())
          .stream()
          .map(
              partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
          .collect(Collectors.toList());

      if ("start".equalsIgnoreCase(position)) {
        consumer.seekToBeginning(topicPartitions);
      }
      if ("end".equalsIgnoreCase(position)) {
        consumer.seekToEnd(topicPartitions);
      }

      consumer.poll(1000);
      consumer.commitSync();
    } catch (Exception e) {
      return e.getMessage();
    } finally {
      consumer.close();
    }
    return "Offset moved to: " + position;
  }

  private KafkaConsumer<String, String> getConsumer(OffSetChangeRequest request) {
    Properties props = new Properties();
    props.put("bootstrap.servers", request.getKafkaBroker());
    props.put("group.id", request.getConsumerGroupId());
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("enable.auto.commit", false);
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("session.timeout.ms", "30000");
    props.put("auto.offset.reset", "earliest");
    return new KafkaConsumer<>(props);
  }
}

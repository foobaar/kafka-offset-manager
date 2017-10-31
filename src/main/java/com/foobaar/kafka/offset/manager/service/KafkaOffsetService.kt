package com.foobaar.kafka.offset.manager.service

import com.foobaar.kafka.offset.manager.dao.OffSetChangeRequest
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.stereotype.Component
import java.util.*

@Component
class KafkaOffsetService {
    fun changeOffset(req: OffSetChangeRequest): String? {
        var consumer: KafkaConsumer<String, String>? = null
        try {
            consumer = getConsumer(req)

            val offsets = HashMap<TopicPartition, OffsetAndMetadata>()
            for (partitionOffset in req.partitionOffsets!!) {
                val topicPartition = TopicPartition(req.topic,
                        partitionOffset.partition)
                val offsetAndMetadata = OffsetAndMetadata(
                        partitionOffset.offset)
                offsets.put(topicPartition, offsetAndMetadata)
            }

            consumer.commitSync(offsets)
        } catch (e: Exception) {
            return e.message
        } finally {
            consumer!!.close()
        }
        return "Offset change complete."
    }

    fun changeOffsetToEnd(req: OffSetChangeRequest, position: String): String? {

        var consumer: KafkaConsumer<String, String>? = null
        try {
            consumer = getConsumer(req)
            /*
      Since we would shut down all consumers before we execute this method, rebalancing should not
      an issue as kafka-offset-handler would be the only consumer.
       */
            consumer.subscribe(setOf(req.topic), NoOpConsumerRebalanceListener())
            consumer.poll(1000)
            val topicPartitions = consumer.partitionsFor(req.topic)
                    .map { TopicPartition(it.topic(), it.partition()) }

            if ("start".equals(position, ignoreCase = true)) {
                consumer.seekToBeginning(topicPartitions)
            }
            if ("end".equals(position, ignoreCase = true)) {
                consumer.seekToEnd(topicPartitions)
            }

            consumer.poll(1000)
            consumer.commitSync()
        } catch (e: Exception) {
            return e.message
        } finally {
            consumer!!.close()
        }
        return "Offset moved to: " + position
    }

    fun changeOffsetBasedOnTimeStamp(req: OffSetChangeRequest): String? {
        var consumer: KafkaConsumer<String, String>? = null
        try {
            consumer = getConsumer(req)
            val partitionInfo = consumer.partitionsFor(req.topic)
            val timeStampsToSearch = partitionInfo
                    .map { TopicPartition(it.topic(), it.partition()) }
                    .map { Pair(it, req.timeStampInMillis) }
                    .toMap()
            val offSetsForTimes = consumer
                    .offsetsForTimes(timeStampsToSearch)
            val offsets = HashMap<TopicPartition, OffsetAndMetadata>()
            offSetsForTimes.entries
                    .forEach { offsets.put(it.key, OffsetAndMetadata(it.value.offset())) }
            consumer.commitSync(offsets)
        } catch (e: Exception) {
            return e.message
        } finally {
            consumer!!.close()
        }
        return "Offset moved to timestamp: " + req.timeStampInMillis
    }

    private fun getConsumer(request: OffSetChangeRequest): KafkaConsumer<String, String> {
        val props = Properties()
        props.put("bootstrap.servers", request.kafkaBroker!!)
        props.put("group.id", request.consumerGroupId!!)
        props.put("key.deserializer", StringDeserializer::class.java.name)
        props.put("enable.auto.commit", false)
        props.put("value.deserializer", StringDeserializer::class.java.name)
        props.put("session.timeout.ms", "30000")
        props.put("auto.offset.reset", "earliest")
        return KafkaConsumer(props)
    }
}

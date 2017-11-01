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
        getConsumer(req).use {
            val offsets = HashMap<TopicPartition, OffsetAndMetadata>()
            for (partitionOffset in req.partitionOffsets!!) {
                val topicPartition = TopicPartition(req.topic, partitionOffset.partition)
                val offsetAndMetadata = OffsetAndMetadata(partitionOffset.offset)
                offsets.put(topicPartition, offsetAndMetadata)
            }
            it.commitSync(offsets)
        }

        return "Offset change complete."
    }

    fun changeOffsetToEnd(req: OffSetChangeRequest, position: String): String? {
        getConsumer(req).use {
            /*
            Since we would shut down all consumers before we execute this method, rebalancing should not
            an issue as kafka-offset-handler would be the only consumer.
            */
            it.subscribe(setOf(req.topic), NoOpConsumerRebalanceListener())
            it.poll(1000)
            val topicPartitions = it.partitionsFor(req.topic)
                    .map { TopicPartition(it.topic(), it.partition()) }

            when (position) {
                "start" -> it.seekToBeginning(topicPartitions)
                "end" -> it.seekToEnd(topicPartitions)
            }

            it.poll(1000)
            it.commitSync()
        }
        return "Offset moved to: " + position
    }

    fun changeOffsetBasedOnTimeStamp(req: OffSetChangeRequest): String? {
        getConsumer(req).use {
            val partitionInfo = it.partitionsFor(req.topic)
            val timeStampsToSearch = partitionInfo
                    .map { TopicPartition(it.topic(), it.partition()) }
                    .map { Pair(it, req.timeStampInMillis) }
                    .toMap()
            val offSetsForTimes = it.offsetsForTimes(timeStampsToSearch)
            val offsets = HashMap<TopicPartition, OffsetAndMetadata>()
            offSetsForTimes.entries.forEach { offsets.put(it.key, OffsetAndMetadata(it.value.offset())) }
            it.commitSync(offsets)
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

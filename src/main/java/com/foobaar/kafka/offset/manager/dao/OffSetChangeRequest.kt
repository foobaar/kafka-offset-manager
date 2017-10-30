package com.foobaar.kafka.offset.manager.dao

class OffSetChangeRequest {
    var consumerGroupId: String? = null
    var topic: String? = null
    var partitionOffsets: List<PartitionOffset>? = null
    var kafkaBroker: String? = null
    var timeStampInMillis: Long = 0
}

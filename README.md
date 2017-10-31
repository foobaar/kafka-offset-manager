## Motivation

- The idea behind the app is to allow you to manually set offsets in kafka.
- You can move to a custom offset or move to the beginning or end of a topic

## Pre req
- kafka version of 0.10.1.0 or greater
- You need to make sure that all consumers that belong to a  consumer group should be stopped before changing offsets.

## Installation

./gradlew bootRun 

## Usage
Setting Custom Offsets
```
POST localhost:8082/kafka-offset-manager/custom-offset
POST BODY:
{
  "consumerGroupId" : "consumer1",
  "kafkaBroker" : "localhost:9092",
  "topic": "test",
  "partitionOffsets": [{"partition": "0", "offset": "29753"}, {"partition": "1", "offset": "30595"}]
}
```

Moving the offsets to the earliest available offset:
```
POST localhost:8082/kafka-offset-manager/boundary?position=start
POST BODY:
{
  "consumerGroupId" : "consumer1",
  "topic": "test",
  "kafkaBroker" : "localhost:9092"
}
```
Moving the offsets to the latest available offset:
```
POST localhost:8082/kafka-offset-manager/boundary?position=end
POST BODY:
{
  "consumerGroupId" : "consumer1",
  "topic": "test",
  "kafkaBroker" : "localhost:9092"
}
```

Moving the offsets to the earliest available offset for a given timestamp
```
POST localhost:8082/kafka-offset-manager/time-based-offset
POST BODY:
{
  "consumerGroupId" : "consumer1",
  "topic": "test",
  "kafkaBroker" : "localhost:9092",
  "timeStampInMillis": "23123123123"
}
```

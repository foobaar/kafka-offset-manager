package com.foobaar.kafka.offset.manager

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class KafkaOffsetManagerApplication

fun main(args: Array<String>) {
    SpringApplication.run(KafkaOffsetManagerApplication::class.java, *args)
}

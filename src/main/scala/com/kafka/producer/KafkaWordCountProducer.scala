package com.kafka.producer

import java.util.Properties

import kafka._

import kafka.producer._
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val meg = new KeyedMessage[String, String]("test", "Hi How are you")
    producer.send(meg)
  }
}

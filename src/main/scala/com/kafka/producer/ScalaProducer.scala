package com.kafka.producer
import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
object ScalaProducer {

  def main(args: Array[String]): Unit = {
    println("how are you")

    val topic = "test"

    val props = new Properties()

    //props.put("metadata.broker.list", "localhost:2181")

    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
    props.put("producer.type", "async")
    //props.put("request.required.acks", "1")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val t = System.currentTimeMillis()
    val runtime = new Date();

    val msg = runtime + "," + ", Nice to See u";
    val data = new KeyedMessage[String, String](topic,"2", msg);
    producer.send(data);
    println("message sent key: " + "2" + " value is:" + msg)
    producer.close();

  }
}
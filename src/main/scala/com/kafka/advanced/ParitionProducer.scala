package com.kafka.advanced
import java.util.Arrays
import java.util.List
import java.util.Properties
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.message.Message
import kafka.producer.SyncProducerConfig
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
import scala.util.Random
import kafka.utils.ZKStringSerializer$
import org.I0Itec.zkclient.ZkClient
import kafka.admin.AdminUtils
import java.util.Properties

object ParitionProducer {
  def main(args: Array[String]) {
    val events = 1000000
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "com.kafka.advanced.SimplePartitioner");

    val config = new ProducerConfig(props);
    val producer = new Producer[String, String](config)
    val zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
    //AdminUtils.createTopic(zkClient, "sekhar", 10, 1, new Properties());
    val topicName = "sekhar"
    val isExist = AdminUtils.topicExists(zkClient, topicName)
    println("Pre Check: " + isExist)

    if (!isExist) {
      AdminUtils.createTopic(zkClient, "sekhar", 2, 1, new Properties());
    }
    val isExistPost = AdminUtils.topicExists(zkClient, topicName)
    println("Post Check: " + isExistPost)
    
    val key = Random.nextInt(7).toString
    val msg = "This is not Recon message !!!"
    //val data = new KeyedMessage(topicName, "RECON", msg);
    val data = new KeyedMessage(topicName, "DELIVERY", msg);
    if (isExistPost) {
      producer.send(data);
      println("Message sent to the topic")
    }

    producer.close();
  }
}
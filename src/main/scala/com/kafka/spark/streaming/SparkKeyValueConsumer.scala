package com.kafka.spark.streaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
object SparkKeyValueConsumer {

  def main(args: Array[String]): Unit = {
    println("hi how are u")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(20))

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

    val topics = Set("test")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      
       stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        // make sure connection pool is set up on the executor before writing

        iter.foreach { case (key, msg) =>
            // the unique key for idempotency is just the text of the message itself, for example purposes
          println("Key is: "+ key +" value is: "+msg)
          }
        }
      }

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

}

package com.kafka.spark.streaming

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object SparkBasicConsumer {
  
  
  def main(args: Array[String]): Unit ={

    //val Array(zkQuorum, group, topics, numThreads) = args
    val zkQuorum = "localhost:2181"
    val group = "test-consumer-group"
    val topicMap = Map("sekhar" -> 2)
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(20))
    ssc.checkpoint("checkpoint")

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    
    // Below is word count
    /*val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(60), 2)
    wordCounts.print()*/
    
    // print line by line
    lines.foreachRDD( rdd => {
    for(item <- rdd.collect().toArray) {
        println(item);
    }
})
    

    ssc.start()
    ssc.awaitTermination()
  }
  
}
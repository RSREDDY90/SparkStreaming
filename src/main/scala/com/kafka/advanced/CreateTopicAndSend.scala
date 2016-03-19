package com.kafka.advanced
import kafka.utils.ZKStringSerializer$
import org.I0Itec.zkclient.ZkClient
import kafka.admin.AdminUtils
import java.util.Properties
object CreateTopicAndSend {
  def main(args: Array[String]): Unit = {
    val zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
    //AdminUtils.createTopic(zkClient, "sekhar", 10, 1, new Properties());
    
    val isExist = AdminUtils.topicExists(zkClient, "test")
    println(isExist)
  }

}
package com.kafka.advanced

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

class SimplePartitioner(var props: VerifiableProperties) extends Partitioner {

  var partition: Int = 0

  def partition(key: Any, numPartitions: Int): Int = {
    if (key.equals("RECON")) {
      println("RECON parition is: " + partition)
      partition
    } else {
      partition = 1
      println("Other parition is: " + partition)
      partition

    }

  }
}
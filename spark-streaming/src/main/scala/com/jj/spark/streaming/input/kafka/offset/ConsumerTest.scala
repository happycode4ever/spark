package com.jj.spark.streaming.input.kafka.offset

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConversions._
object ConsumerTest {
  def main(args: Array[String]): Unit = {
    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaGroup",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    val consumer = new KafkaConsumer[String,String](kafkaParam)
    consumer.subscribe(List("offsettest"))
    while(true){
      consumer.poll(100).foreach(record=>
        println(s"topic:${record.topic},partition:${record.partition},offset:${record.offset()},content:${record.value()}"))
    }
  }
}

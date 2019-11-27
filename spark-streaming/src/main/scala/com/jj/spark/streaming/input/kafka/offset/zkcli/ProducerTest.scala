package com.jj.spark.streaming.input.kafka.offset.zkcli

import java.time.LocalDateTime

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.JavaConversions._
import scala.util.Random

object ProducerTest {
  def main(args: Array[String]): Unit = {
    val topic = "offsettest"
    val kafkaParam = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
    )
    val producer = new KafkaProducer[String,String](kafkaParam)
    val random = new Random()
    while(true){
      val content = LocalDateTime.now().toString + ":" +random.nextInt(100000)
      producer.send(new ProducerRecord[String,String](topic,content),new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = println(s"topic:${metadata.topic},partition:${metadata.partition},offset:${metadata.offset()},content:$content")
      })
      Thread.sleep(3000)
    }
  }
}

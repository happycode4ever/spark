package com.jj.spark.streaming.input.kafka.offset.zkcli

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectStreamingTest {
  private val sparkConf = new SparkConf().setMaster("local[4]").setAppName("DirectStreamingTest2")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  def main(args: Array[String]): Unit = {
    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaGroup",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    val sourceDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,Set("offsettest"))
    sourceDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(items=>{
        for(item <- items)println(item)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

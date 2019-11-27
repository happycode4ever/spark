package com.jj.spark.streaming.input.kafka.offset.kafkacluster2

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{DefaultKafkaManager, HasOffsetRanges, KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectStreamingTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("DirectStreamingTest")
      //设置streaming从每个分区每秒获取的条数 方便调试
      .set("spark.streaming.kafka.maxRatePerPartition","1")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val kafkaParams = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.GROUP_ID_CONFIG -> "customGroup2",
//      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

    val topics = Set("custom_offset")
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).get
//    val kafkaManager = new KafkaManagerTest(kafkaParams,topics,groupId)
    val kafkaManager = new DefaultKafkaManager(kafkaParams)

    val kafkaDStream = kafkaManager.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    kafkaDStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(range => println(s"streaming range -> $range"))
     rdd.foreach({case (k,v) => println(s"key -> $k, value -> $v")})
      kafkaManager.updateZKOffsets(rdd)
    })

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }

}

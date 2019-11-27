package com.jj.spark.streaming.input.kafka.offset.kafkacluster

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, KafkaUtils}
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
      ConsumerConfig.GROUP_ID_CONFIG -> "customGroup",
//      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

    val topics = Set("custom_offset")
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).get
//    val kafkaManager = new KafkaManagerTest(kafkaParams,topics,groupId)
    val kafkaManager = new KafkaManager(kafkaParams)
    /*
    def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      messageHandler: MessageAndMetadata[K, V] => R
  ): InputDStream[R]
     */
    //手动设置要开始消费的偏移 该偏移应该从zk获取上次成功消费的offset

    //初始化起始的offset
    val fromOffsets = kafkaManager.initFromOffsetToZk(topics,groupId)
    val kafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
      ssc,
      kafkaParams,
      fromOffsets,
      (messageHandler:MessageAndMetadata[String,String])=>(messageHandler.key(),messageHandler.message()))

    val kafkaDStream2 = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

    //streaming具体业务处理，注意rdd之前不要有转换操作
    kafkaDStream.foreachRDD(rdd=>{
      //获取spark每批rdd处理的offset区间
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offsetRange =>{
        println(s"streaming offsetRange -> ${offsetRange}")
      })
//      kafkaManager.getOffsetInfo
      //对于每个rdd处理的业务
      rdd.foreach({case (key,value) => println(s"consume key -> $key , value -> $value")})
      //业务处理完成后需要记录zk
      //构建需要记录的offsets
      val offsets = offsetRanges.map(offsetRange => {
        (TopicAndPartition(offsetRange.topic,offsetRange.partition),offsetRange.untilOffset)
      }).toMap
      kafkaManager.setConsumerGroupOffset(topics,groupId,offsets)
    })

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }

}

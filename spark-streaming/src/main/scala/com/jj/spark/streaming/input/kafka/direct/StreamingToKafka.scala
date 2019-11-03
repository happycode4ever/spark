package com.jj.spark.streaming.input.kafka.direct

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingToKafka {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingToKafka")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  def main(args: Array[String]): Unit = {
    val kafkaProp:Map[String,String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaGroup",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    val fromTopic = "from"
    val toTopic = "to"

    //建立和kafka的连接
    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaProp,Set(fromTopic))
    val resultDStream = stream.map({case (key,value) => "value:"+value})
    resultDStream.foreachRDD({
      rdd =>{
        rdd.foreachPartition( items =>{

          for(item <- items){
            //写回kafka需要连接池
            val pool = KafkaPool("hadoop112:9092,hadoop113:9092,hadoop114:9092")
            val kafkaProxy = pool.borrowObject()

            //使用代理对象的kafka客户端发送消息
            kafkaProxy.kafkaClient.send(new ProducerRecord[String,String](toTopic,item))

            //使用完放回连接池
            pool.returnObject(kafkaProxy)
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

class KafkaManagerTest(kafkaParams:Map[String,String], topics:Set[String], groupId:String) extends Serializable {
  private val kafkaCluster = new KafkaCluster(kafkaParams)
  private val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topics)

  def getOffsetInfo: Unit ={
    if(topicAndPartitionEither.isRight){
      val topicAndPartition = topicAndPartitionEither.right.get
      val earliestOffsetMap = kafkaCluster.getEarliestLeaderOffsets(topicAndPartition).right.get
      //获取kafka最小偏移 包含leader分区所在broker的信息和offset
      earliestOffsetMap.foreach({case (topicAndPartition: TopicAndPartition,leaderOffset: LeaderOffset) => {
        println(s"earliestOffsetMap -> topicAndPartition:$topicAndPartition,leaderOffset:$leaderOffset")
      }})
      //获取kafka最大偏移
      val latestOffsetMap = kafkaCluster.getLatestLeaderOffsets(topicAndPartition).right.get
      latestOffsetMap.foreach({case (topicAndPartition: TopicAndPartition,leaderOffset: LeaderOffset) =>
        println(s"latestOffsetMap -> topicAndPartition:$topicAndPartition,leaderOffset:$leaderOffset")
      })
      //获取zk上消费者已消费的偏移
      val consumerOffsetsEither = kafkaCluster.getConsumerOffsets(groupId,topicAndPartition)
      if(consumerOffsetsEither.isLeft){
        val err = consumerOffsetsEither.left.get
        println(s"groupId:$groupId no cosumerInfo on zookeeper!!")
//        err.foreach(e=>e.printStackTrace())
      }
      if(consumerOffsetsEither.isRight){
        val consumerOffsetsMap = consumerOffsetsEither.right.get
        consumerOffsetsMap.foreach({case (topicAndPartition: TopicAndPartition,offset: Long) =>
          println(s"grouId:$groupId consumeInfo -> $topicAndPartition,offset:$offset")
        })
      }
    }else{
      val err = topicAndPartitionEither.left.get
      println(s"no topic info!!! :$topics")
//      err.foreach(e => e.printStackTrace())
    }
  }

}

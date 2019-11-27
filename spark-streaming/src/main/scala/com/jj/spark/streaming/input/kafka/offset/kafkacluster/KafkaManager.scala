package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

class KafkaManager(kafkaParams: Map[String, String]) {
  private val kafkaCluster = new KafkaCluster(kafkaParams)

  //获取指定topic的相关分区信息
  def getTopicAndPartition(topics: Set[String]): Set[TopicAndPartition] = {
    val topicAndPartitionEither = kafkaCluster.getPartitions(topics)
    if (topicAndPartitionEither.isLeft) {
      throw new SparkException(s"missing topics:$topics")
    } else {
      topicAndPartitionEither.right.get
    }
  }

  //获取topic的最小偏移
  def getEarliestOffsetMap(topics: Set[String]): Map[TopicAndPartition, LeaderOffset] = {
    val topicAndPartitions = getTopicAndPartition(topics)
    val earliestOffsetInfoEither = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    val earliestOffsetMap = earliestOffsetInfoEither.right.get
    earliestOffsetMap
  }

  //获取topic的最大偏移
  def getLatestOffsetMap(topics: Set[String]): Map[TopicAndPartition, LeaderOffset] = {
    val topicAndPartitions = getTopicAndPartition(topics)
    val latestOffsetInfoEither = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
    val latestOffsetMap = latestOffsetInfoEither.right.get
    latestOffsetMap
  }

  //获取消费者组的偏移
  def getConsumerGroupOffsetMap(topics: Set[String], groupId: String):Map[TopicAndPartition,Long] = {
    val topicAndPartitions = getTopicAndPartition(topics)
    val groupOffsetInfoEither = kafkaCluster.getConsumerOffsets(groupId, topicAndPartitions)
    if (groupOffsetInfoEither.isRight) {
      val groupOffsetMap = groupOffsetInfoEither.right.get
      groupOffsetMap
    } else {
      null
    }
  }
  //向ZK按照消费者组更新offset
  def setConsumerGroupOffset(topics: Set[String], groupId: String, offsets: Map[TopicAndPartition, Long]) = {
    val topicAndPartitions = getTopicAndPartition(topics)
    kafkaCluster.setConsumerOffsets(groupId, offsets)
  }
  //获取每个分区的起始偏移
  def initFromOffsetToZk(topics:Set[String],groupId:String):Map[TopicAndPartition,Long]={
    //从zk获取上次消费者组的偏移，比对当前kafka的最大最小偏移
    //如果没有说明没消费过，统一置为0
    //如果zk偏移小于最小偏移，更新为最小偏移
    //如果zk偏移大于最大偏移，更新为最大偏移
    var offsets:Map[TopicAndPartition,Long] = null
    val topicAndPartitions = getTopicAndPartition(topics)
    //获取消费者偏移
    val groupOffsetMap = getConsumerGroupOffsetMap(topics,groupId)
    //没有消费过
    if(groupOffsetMap == null){
      //如果没有说明没消费过，统一置为0
      offsets = topicAndPartitions.map(topicAndPartitions => (topicAndPartitions,0L)).toMap
    }else{
      //最小偏移
      val earliestOffsetMap = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions).right.get
      //最大偏移
      val latestOffsetMap = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions).right.get
      offsets = groupOffsetMap.map({case (topicAndPartition: TopicAndPartition,consumerOffset:Long) => {
        //做比较
        val earliestOffset = earliestOffsetMap.get(topicAndPartition).get.offset
        val latestOffset = latestOffsetMap.get(topicAndPartition).get.offset
        //如果zk偏移小于最小偏移，更新为最小偏移，
        if(consumerOffset < earliestOffset){
          (topicAndPartition,earliestOffset)
          //如果zk偏移大于最大偏移，更新为最大偏移
        }else if(consumerOffset > latestOffset){
          (topicAndPartition,latestOffset)
        }else{
          (topicAndPartition,consumerOffset)
        }
      }})
    }
    //最后更新zk
    kafkaCluster.setConsumerOffsets(groupId,offsets)
    //返回更新的offsets信息给direct streaming
    println(s"from offsets -> $offsets")
    offsets
  }

}

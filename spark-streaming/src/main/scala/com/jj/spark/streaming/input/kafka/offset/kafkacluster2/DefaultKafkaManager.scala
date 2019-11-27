package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class DefaultKafkaManager(val kafkaParams: Map[String, String]) {
  private val logger =LoggerFactory.getLogger(KafkaCluster.getClass)
  private val kc = new KafkaCluster(kafkaParams)

  /** 需要自己重载这个方法。以下是该方法的说明：https://github.com/apache/spark/blob/v1.6.0/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    *  - No receivers: This stream does not use any receiver. It directly queries Kafka
    *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
    *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
    *    You can access the offsets used in each batch from the generated RDDs (see
    *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
    *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    *    in the [[StreamingContext]]. The information on consumed offset can be
    *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    *  - End-to-end semantics: This stream ensures that every records is effectively received and
    *    transformed exactly once, but gives no guarantees on whether the transformed data are
    *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    *    that the output operation is idempotent, or use transactions to output records atomically.
    *    See the programming guide for more details.
    *
    * @param ssc StreamingContext object
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
    *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
    *   host1:port1,host2:port2 form.
    *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
    *   to determine where the stream starts (defaults to "largest")
    * @param topics Names of the topics to consume
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    * @tparam KD type of Kafka message key decoder
    * @tparam VD type of Kafka message value decoder
    * @return DStream of (Kafka message key, Kafka message value)
    */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag]
  ( ssc: StreamingContext, kafkaParams: Map[String, String],
    topics: Set[String]
  ): InputDStream[(K, V)] =  {

    val groupId = kafkaParams.get("group.id").get
    // 在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)

    //从zookeeper上读取offset开始消费message
    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft)
        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    }
    messages
  }

  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics   topics
    * @param groupId  consumer group id
    */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {// 消费过
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case(tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            logger.warn("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else {// 没有消费过
      val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  /**
    * 更新zookeeper上的消费offsets
    * @param rdd rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String)]) : Unit = {
    val groupId = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      logger.warn("update offset ..................................................")
      if (o.isLeft) {
        logger.warn(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
    logger.warn("end  update offset ..................................................")
  }

}

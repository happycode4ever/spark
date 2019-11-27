package com.jj.spark.streaming.input.kafka.offset.zkcli

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OffsetTest {
  private val sparkConf = new SparkConf().setMaster("local[4]").setAppName("OffsetTest")
  private val ssc = new StreamingContext(sparkConf, Seconds(5))
  private val zookeeper = "hadoop112:2181,hadoop113:2181,hadoop114:2181"
  private val consumerGroup = "kafkaGroup"
  private val topic = "offsettest"

  def main(args: Array[String]): Unit = {
    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop112:9092,hadoop113:9092,hadoop114:9092",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    //    val topic : String = "offsettest"   //消费的 topic 名字
    val topics: Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合

    val topicDirs = new ZKGroupTopicDirs(consumerGroup, topic) //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}" //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name

    val zkClient = new ZkClient(zookeeper) //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var kafkaStream: InputDStream[(String, String)] = null

    if (children > 0) { //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      var fromOffsets: Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
      println("从zookeeper恢复")

      val req = new TopicMetadataRequest(List(topic), 0)
      //创建一个获取元信息的请求
      val getLeaderConsumer = new SimpleConsumer("hadoop114", 9092, 10000, 10000, "OffsetLookup") // 第一个参数是 kafka broker随机一个的host，第二个是 port
      val res = getLeaderConsumer.send(req)
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String] // 将结果转化为 partition -> leader 的映射关系，也就是分区号与主机名
        case None =>
          Map[Int, String]()
      }
      getLeaderConsumer.close()

      println(s"partitions info : $partitions")
      println(s"children info : $children")

      for (i <- 0 until children) {
        //        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        //        val tp = TopicAndPartition(topic, i)
        //        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中

        //获取保存在zk中的偏移信息
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        zkClient.close()
        println(s"partition[$i]在zk保存的偏移位置是:$partitionOffset")

        //获取kafka当前partition的最小offset（主要防止kafka中的数据过期问题）
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 10000, 10000, "getMinOffset")
        val response = consumerMin.getOffsetsBefore(requestMin)
        //获取当前的偏移量
        val curOffsets = response.partitionErrorAndOffsets(tp).offsets
        consumerMin.close()

        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) { // 通过比较从 kafka 上该 partition 的最小 offset 和 zk 上保存的 offset，进行选择,如果之前zk保存的offset已经过期，需要重置成当前最小的offset
          nextOffset = curOffsets.head
        }
        println(s"partition[$i]在kafka保存的最小偏移位置是:${curOffsets.head}")
        println(s"partition[$i]修正的偏移位置是:$nextOffset")
        fromOffsets += (tp -> nextOffset) //设置正确的 offset，这里将 nextOffset 设置为 0（0 只是一个特殊值），可以观察到 offset 过期的现象
        println("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
    }
    else {
      println("直接创建")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
    }

    //用于存储偏移量
    var offsetRanges = Array[OffsetRange]()

    //注意transform不要有额外操作
    val mapStream = kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(x => x._2)

    mapStream.foreachRDD { rdd =>
      //具体的业务逻辑
      rdd.foreachPartition(
        message => {
          while (message.hasNext) {
            println(s"@^_^@   [" + message.next() + "] @^_^@")
          }
        }
      )

      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        val updateZkClient = new ZkClient(zookeeper)
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
        updateZkClient.close()
        println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

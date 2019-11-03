package com.jj.spark.streaming.input.flume.pull

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePullTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(3))

  def main(args: Array[String]): Unit = {
    //推模式
    //较新的方式是拉式接收器(在Spark 1.1中引入)，它设置了一个专用的Flume数据池供 Spark Streaming读取，并让接收器主动从数据池中拉取数据。
    // 这种方式的优点在于弹性较 好，Spark Streaming通过事务从数据池中读取并复制数据。在收到事务完成的通知前，这 些数据还保留在数据池中。
    //**1.使用注意flume配置的sink是到spark提供的连接池，所以需要依赖spark-streaming-flume-sink_2.11-2.1.1.jar该jar包，传到flume的lib下
    //**2.是注意flume使用的scala版本需要和项目匹配
    //**3.启动方式是先起flume将数据存到对应IP和port上的spark sink 然后再起这个spark app 和push模式启动顺序相反

    //API只是将createStream换成了createPollingStream
    val sourceDStream = FlumeUtils.createPollingStream(ssc,"hadoop114",12345,StorageLevel.MEMORY_ONLY)
    val resultDStream = sourceDStream.map(event=>new String(event.event.getBody.array()))
    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

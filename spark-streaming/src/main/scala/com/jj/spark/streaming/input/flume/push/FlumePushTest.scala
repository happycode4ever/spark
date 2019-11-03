package com.jj.spark.streaming.input.flume.push

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePushTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(3))

  /**
    * 推模式存在丢失数据的风险
    * 虽然这种方式很简洁，但缺点是没有事务支持。这会增加运行接收器的工作节点发生错误 时丢失少量数据的几率。
    * 不仅如此，如果运行接收器的工作节点发生故障，系统会尝试从 另一个位置启动接收器，这时需要重新配置 Flume 才能将数据发给新的工作节点。这样配置会比较麻烦。
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //使用FlumeUtils创建DStream，注意Flume的conf对应的sink是Avro，而且这边的端口必须先创建才能开flume，中间参数填上对接的IP和端口
    //该模式不推荐使用，sparkstreaming压力过大，注意启动顺序是spark app->flume
    val sourceDStream = FlumeUtils.createStream(ssc,"192.168.0.106",9999,StorageLevel.MEMORY_ONLY)
    //实际上采集到的是包装的Flume的event需要event.event就能提取head和body的内容做业务的转换
    //这里是直接读取event的body内容转换字符串输出
    val resultDStream = sourceDStream.map(event=>new String(event.event.getBody.array()))
    resultDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

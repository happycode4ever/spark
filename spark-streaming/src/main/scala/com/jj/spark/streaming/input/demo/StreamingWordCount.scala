package com.jj.spark.streaming.input.demo

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {
  //注意sprakSreaming使用的线程数不能低于2个最少都是local[2]
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
  //根据conf和时间短微批次处理数据流，时间单位有毫秒，秒，分
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  def main(args: Array[String]): Unit = {
    //socket连接监听端口的输入socketTextStream
    val lineDSteam = ssc.socketTextStream("hadoop113",9999,StorageLevel.MEMORY_ONLY)
    val wordDStream = lineDSteam.flatMap(_.split(" "))
    val countDStream = wordDStream.map((_,1))
    val resultDStream = countDStream.reduceByKey(_+_)
    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

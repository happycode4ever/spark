package com.jj.spark.streaming.input.custom

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CustomStreamingTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomStreamingTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))
  def main(args: Array[String]): Unit = {
    //通过receiverStream使用自定义采集器
    val lineDStream = ssc.receiverStream(new CustomReceiver("hadoop113",9999))
    val resultDStream = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

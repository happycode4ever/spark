package com.jj.spark.streaming.input.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreamingWordCount {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FileStreamingWordCount")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  def main(args: Array[String]): Unit = {
    //监控HDFS文件变化，用处不大
    val fileDStream = ssc.textFileStream("hdfs://hadoop112:9000/spark/data")
    val wordDStream = fileDStream.flatMap(_.split(" "))
    val countDStream = wordDStream.map((_,1))
    val resultDStream = countDStream.reduceByKey(_+_)
    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

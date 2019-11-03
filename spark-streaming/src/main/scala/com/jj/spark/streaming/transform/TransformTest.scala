package com.jj.spark.streaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))
  def main(args: Array[String]): Unit = {
    val sourceDStream = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY)
    //无状态转换离散的RDD各自独立 将DStream里的每个离散的RDD转换成新的RDD再包装回rdd返回
    val resDStream = sourceDStream.transform(rdd=>{
      rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    })
    resDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

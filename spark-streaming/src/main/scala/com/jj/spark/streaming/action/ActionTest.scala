package com.jj.spark.streaming.action

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ActionTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  //注意DStream一定要有action输出不然没法执行
  def main(args: Array[String]): Unit = {
    val sourceDSteam = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY)
    val mapDStream = sourceDSteam.flatMap(_.split(" "))

    //count 统计value总数
    val res1 = mapDStream.count()
    //countByValue相当于map((_,1)).reduceByKey(_+_)
    val res2 = mapDStream.countByValue()
//    res1.print()
//    res2.print()

    //最终调用的还是rdd的saveAsTextFile
    //def saveAsTextFiles(prefix: String, suffix: String = ""): Unit
//    res2.saveAsTextFiles("./action/pre","log")

    //最常用的是foreachRDD
    res2.foreachRDD(rdd=>{
      rdd.foreach(x=>println(x._1,x._2))
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

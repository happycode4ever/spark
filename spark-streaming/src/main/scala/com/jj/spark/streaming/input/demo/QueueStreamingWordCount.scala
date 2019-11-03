package com.jj.spark.streaming.input.demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueStreamingWordCount {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("QueueStreamingWordCount")
  private val ssc = new StreamingContext(sparkConf,Seconds(3))

  def main(args: Array[String]): Unit = {
    println("start main")
    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    //创建RDD队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    // 创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue)

    //处理队列中的RDD数据
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    //打印结果
    reducedStream.print()
    //启动计算
    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
      //通过程序停止StreamingContext的运行
//      ssc.stop()
//    ssc.start()
      ssc.awaitTermination()
  }
}

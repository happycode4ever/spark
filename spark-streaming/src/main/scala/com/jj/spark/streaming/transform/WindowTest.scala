package com.jj.spark.streaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  def main(args: Array[String]): Unit = {
    val sourceDStream = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY)
    val mapDStream = sourceDStream.flatMap(_.split(" ")).map((_,1))

    //windowDuration窗口大小 slideDuration滑动步长 这两个单位必须是ssc的duration的整数倍 不然rdd没法被切分，注意ssc的一个duration对应一个rdd
//    def reduceByKeyAndWindow(reduceFunc: (V, V) => V,windowDuration: Duration,slideDuration: Duration): DStream[(K, V)]
//    val resDStream = mapDStream.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Seconds(10),Seconds(5))

    //优化函数  必须使用checkpoint
    ssc.checkpoint("./windowcheck")
    //那么上图中，在time5的时候，reduceFunc处理的数据就是time4和time5；invReduceFunc处理的数据就是time1和time2。
    // 此处需要特别特别特别处理，这里的window at time 5要理解成time 5的最后一刻，如果这里的time是一秒的话，那么time 5其实就是第5秒最后一刻，也就是第6秒初。
//    def reduceByKeyAndWindow(reduceFunc: (V, V) => V,invReduceFunc: (V, V) => V,windowDuration: Duration,slideDuration: Duration = self.slideDuration,
    val resDStream = mapDStream.reduceByKeyAndWindow(_+_,_-_,Seconds(10),Seconds(5))
    resDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

package com.jj.spark.streaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(5))

  def main(args: Array[String]): Unit = {
    val sourceDStream = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY)
    val mapDStream = sourceDStream.flatMap(_.split(" ")).map((_,1))

    //该函数由于ByKey所以针对KV类型的DStream也就属于PairDStreamFunctions.updateStateByKey
    //入参seq就是bykey聚集的value，入参Option[S]就是已经记录的状态，返回参数也是Option[S]表示需要返回的状态
    //注意S是值类型，也可以使用元组或者样例类包含多个状态例如(count,sum,avg...),另外由于S是自定义的类型需要显示指明不然scala编译报错
//    def updateStateByKey[S: ClassTag](updateFunc: (Seq[V], Option[S]) => Option[S]): DStream[(K, S)]

    //需要开启检查点保存状态默认是使用hadoop的API所以本地调试需要配置HADOOP_HOME
    //本地调试的当前目录是在父parent的根目录下
    ssc.checkpoint("./checkpoint")

    val resDStream = mapDStream.updateStateByKey((seq,state:Option[Int])=>{
      state match {
          //如果之前有状态叠加 目前聚集的key的count之和 + 之前的状态值
        case Some(tmp) => Some(seq.sum + tmp)
          //之前没状态的话直接返回目前这个key叠加的count之和
        case None => Some(seq.sum)
      }
    })
    resDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

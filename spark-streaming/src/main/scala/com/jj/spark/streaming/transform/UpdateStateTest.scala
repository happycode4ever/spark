package com.jj.spark.streaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object UpdateStateTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateTest")
  private val ssc = new StreamingContext(sparkConf,Seconds(2))

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

    val resDStream = mapDStream.updateStateByKey[Int]((seq:Seq[Int],state:Option[Int])=>{
      state match {
          //如果之前有状态叠加 目前聚集的key的count之和 + 之前的状态值
        case Some(tmp) => Some(seq.sum + tmp)
          //之前没状态的话直接返回目前这个key叠加的count之和
        case None => Some(seq.sum)
      }
    })
    //updateStateByKey输出结果每批一直会保留之前的统计结果，即使没有输入
    //------------------
    //(aa,1)
    //(a,13)
    //------------------
    //(aa,1)
    //(a,18)
    //------------------
    //(aa,1)
    //(a,19)
    //------------------
    resDStream.foreachRDD(rdd=>{
      rdd.foreach(println)
      println("------------------")
    })

    def mapFun(key:String,value:Option[Int],state:State[Int]):(String,Int)={
      val sum = value.get + state.getOption().getOrElse(0)
      state.update(sum)
      (key,sum)
    }
    //函数名 _代表返回的是函数本身
//        val f:(String,Option[Int],State[Int])=>(String,Int) = mapFun _
        val resDStream2 = mapDStream.mapWithState(StateSpec.function(mapFun _))

    //输出结果每批有输入才会显示叠加的结果，并且多少个输入就对应多少个输出，如果没有输入就没有输出
    //--------------------
    //2019-12-25 20:57:44,059   WARN --- [Executor task launch worker for task 5]  org.apache.spark.executor.Executor(line:66) : 1 block locks were not released by TID = 5:
    //[rdd_11_0]
    //(a,2)
    //2019-12-25 20:57:44,068   WARN --- [Executor task launch worker for task 6]  org.apache.spark.executor.Executor(line:66) : 1 block locks were not released by TID = 6:
    //[rdd_11_1]
    //--------------------
    //2019-12-25 20:57:46,114   WARN --- [Executor task launch worker for task 12]  org.apache.spark.executor.Executor(line:66) : 1 block locks were not released by TID = 12:
    //[rdd_17_0]
    //(a,3)
    //(a,4)
    //(a,5)
    //(a,6)
    //(a,7)
    //2019-12-25 20:57:46,121   WARN --- [Executor task launch worker for task 13]  org.apache.spark.executor.Executor(line:66) : 1 block locks were not released by TID = 13:
    //[rdd_17_1]
    //--------------------
//    resDStream2.foreachRDD(rdd=>{
//      rdd.foreach(println)
//      println("--------------------")
//    })
    ssc.start()
    ssc.awaitTermination()
  }
}

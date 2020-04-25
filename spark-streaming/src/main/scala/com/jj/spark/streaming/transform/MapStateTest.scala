package com.jj.spark.streaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import redis.clients.jedis.Jedis

object MapStateTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MapStateTest")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    ssc.checkpoint("./mapstatecheck")
    ssc.sparkContext.setLogLevel("error")

    val dataStream = ssc.socketTextStream("localhost",9999)
    val countStream = dataStream.flatMap(_.split(" ")).map((_,1L))
    val mapFunciton = (key:String,value:Option[Long],state:State[Long]) => {
      val accuSum = value.getOrElse(0L)+state.getOption().getOrElse(0L)
      state.update(accuSum)
      (key,accuSum)
    }
//    val resultDStream = countStream.mapWithState(StateSpec.function(mapFunciton)).stateSnapshots()

    val resultDStream = countStream.updateStateByKey((seq:Seq[Long],state:Option[Long]) => {
      Some(seq.sum + state.getOrElse(0L))
    })
    resultDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

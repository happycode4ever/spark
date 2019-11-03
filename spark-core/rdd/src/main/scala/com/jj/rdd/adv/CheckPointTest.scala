package com.jj.rdd.adv

import org.apache.spark.{SparkConf, SparkContext}

object CheckPointTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("cache")
  private val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    sc.setCheckpointDir("hdfs://hadoop112:9000/spark/rdd/checkpoint")
    val rdd = sc.makeRDD(1 to 10).map(_.toString + "[" + System.currentTimeMillis() + "]")
    //开启检查点
    rdd.checkpoint()
    //设置HDFS检查点位置
    val rdd2 = sc.makeRDD(1 to 10).map(_.toString + "[" + System.currentTimeMillis() + "]")
    for (i <- 1 to 5) {
      //也是action操作触发检查点执行，第一次和第二次结果会不一样，后面一致
      println(rdd.collect().mkString(","))
    }
    println("--------------------")
    for (i <- 1 to 5){
      println(rdd2.collect().mkString("#"))
    }
    sc.stop()
  }
}

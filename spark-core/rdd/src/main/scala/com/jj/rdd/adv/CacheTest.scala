package com.jj.rdd.adv

import org.apache.spark.{SparkConf, SparkContext}

object CacheTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("cache")
  private val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {

    val rdd = sc.makeRDD(1 to 10).map(_.toString+"["+System.currentTimeMillis()+"]")
    for(i<- 1 to 5){
      //不缓存的话每次action操作都会重新计算这个rdd
      println(rdd.collect().mkString(","))
    }
    println("------------------")
    //缓存结果之后会将这个rdd缓存到内存里
    //每次action触发都是提取缓存内容
    //cache默认就是def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)
    //持久化类型有内存 磁盘 序列化 以及内存+磁盘(可选序列化) //序列化可以节省空间但是提升了cpu的时间
    val rdd2 = sc.makeRDD(1 to 10).map(_.toString+"["+System.currentTimeMillis()+"]")
    rdd2.cache()
    for(i <- 1 to 5){
      //cache也是action操作触发执行的
      println(rdd2.collect().mkString("#"))
    }

  }
}

package com.jj.rdd.util

import org.apache.spark.rdd.RDD

object PartitionUtil {
  def printRdd[T](rdd:RDD[T])={
    val newRdd = rdd.mapPartitionsWithIndex((index,items)=>{
      Iterator(index+":["+items.mkString(",")+"]")
    }).sortBy(str=>str.substring(0,str.indexOf(":")))
    newRdd.collect().foreach(println)
    newRdd
  }


}

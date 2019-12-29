package com.jj.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object IteratorTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("IterableTest")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(Array(1,2,3,4,5))
    rdd.foreachPartition((items:Iterator[Int]) => {
      //注意foreachPartition是将每个分区作为迭代器返回，遍历过之后迭代器会清空
      //如果需要多次迭代需要先保存为临时变量

//      println(items.min)
//      println(items.max)
//      items.foreach(println)
      val list = items.toList
      println(list.min)
      println(list.max)
      list.foreach(println)
      println("----------------------")
    })
  }
}

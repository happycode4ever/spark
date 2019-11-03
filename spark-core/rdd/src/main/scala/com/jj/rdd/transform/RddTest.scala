package com.jj.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RddTest {
  val sparkConf = new SparkConf().setAppName("aggregateTest").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  def printPatitionInfo[K,V](rdd:RDD[(K,V)])={
    val rddWithIndex = rdd.mapPartitionsWithIndex((index, items) => Iterator(index + ":" + items.mkString(",")))
    println(rddWithIndex.collect().mkString(","))
  }

  def aggregateByKeyTest = {

    val scores = Array(("Fred", 88), ("Fred", 95), ("Mary", 66), ("Fred", 91), ("Wilma", 93), ("Mary", 99), ("Wilma", 95), ("Wilma", 98))
    val rdd = sc.makeRDD(scores)

    //可以更改分区规则
    val hashRdd = rdd.partitionBy(new HashPartitioner(3))
    printPatitionInfo(hashRdd)
    println(s"size = ${rdd.partitions.size}")
    //aggregate方式求总和与计数
    val result1 = rdd.aggregateByKey((0, 0))((u: (Int, Int), v) => (u._1 + v, u._2 + 1), (u1: (Int, Int), u2: (Int, Int)) => (u1._1 + u2._1, u1._2 + u2._2))
    println(s"result1=${result1.collect().mkString(",")}")
    //combine方式求总和与计数
    val result2 = rdd.combineByKey(v => (0, 0), (c: (Int, Int), v) => (c._1 + v, c._2 + 1), (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c1._1, c2._2 + c2._2))
    println(s"result2=${result2.collect().mkString(",")}")
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
//    val data = List(("a" -> 1), ("b" -> 1), ("b" -> 2))
//    val result = data.map { case (k, v) => (k + "aa", v + 10) }
//    val data2 = Map(("a" -> 1), ("b" -> 1), ("b" -> 2))
//
//    def add(k: String, v: Int) = (k, v)

//    aggregateByKeyTest
    val rdd = sc.makeRDD(List(Edge(1,"aaa"),Edge(2,"cccc"),Edge(3,"dddd"))).map({case Edge(id, attr) => attr})
  }
}
case class Edge[ED](id:Long,attr:ED)
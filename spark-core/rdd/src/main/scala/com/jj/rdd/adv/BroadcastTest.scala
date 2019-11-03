package com.jj.rdd.adv

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 大意是， 使用广播变量，每个Executor的内存中，只驻留一份变量副本， 而不是对每个 task 都传输一次大变量，省了很多的网络传输， 对性能提升具有很大帮助， 而且会通过高效的广播算法来减少传输代价。
  *
  * 　　使用广播变量的场景很多， 我们都知道spark 一种常见的优化方式就是小表广播， 使用 map join 来代替 reduce join， 我们通过把小的数据集广播到各个节点上，节省了一次特别 expensive 的 shuffle 操作。
  *
  * 　　比如driver 上有一张数据量很小的表， 其他节点上的task 都需要 lookup 这张表， 那么 driver 可以先把这张表 copy 到这些节点，这样 task 就可以在本地查表了。
  */
object BroadcastTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
  private val sc = new SparkContext(sparkConf)
  def main(args: Array[String]): Unit = {

    val arr = Array(1,2,3,4,5)//百兆级数据如果在多个rdd都有使用到，那么会拷贝这个本地变量到各个executor
    val bc = sc.broadcast(arr)
    println(bc.value.mkString)
    val arr2 = arr.map(_*2)
    val bc2 = sc.broadcast(arr2)
    println(bc.value.mkString)
    println(bc2.value.mkString)
  }
}

package com.jj.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object ActionTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
  private val sc = new SparkContext(sparkConf)

  /**
    * 只要不是返回Rdd的都是action操作
    * reduce针对值类型返回具体的一个T
    * def reduce(f: (T, T) => T): T
    */
  def reduceTest: Unit ={
    val rdd1 = sc.makeRDD(1 to 10,2)
    val res = rdd1.reduce(_ + _)
    println(res)
    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    val res2 = rdd2.reduce((t1,t2) => (t1._1+t2._1,t1._2+t2._2))
    println(res2)
    sc.stop()
  }

  /**
    * collect返回Array[T]
    * def collect(): Array[T]
    */
  def collectTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val res = rdd.collect()
    println(res)
  }

  /**
    * 取出Rdd第一个元素
    *   def first(): T
    *
    */

  def firstTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val i = rdd.first()
    println(i)
  }

  /**
    * 取出Rdd前n个元素
    * def take(num: Int): Array[T]
    */
  def takeTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val ints = rdd.take(5)
    println(ints.mkString(","))
  }

  /**
    * 抽样操作
    * def takeSample(
    * withReplacement: Boolean,
    * num: Int,
    * seed: Long = Utils.random.nextLong): Array[T]
    */
  def takeSampleTest: Unit ={
    val rdd = sc.makeRDD(1 to 100)
//    for(i<- 1 to 5) {
      val ints = rdd.takeSample(false, 10)
      println(ints.mkString(","))
//    }
  }

  /**
    * 排序后返回前n个
    * def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
    */
  def takeOrderedTest: Unit ={
    val rdd = sc.makeRDD(Array(1,6,3,5,7,8,2,9))
    val ints = rdd.takeOrdered(4)
    println(ints.mkString(","))
  }

  /**
    * 针对值类型 类似aggregateByKey
    * def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
    */
  def aggregateTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val sum = rdd.aggregate(0)((res,value)=> res+value,(res1,res2)=>res1+res2)
    //    val sum = rdd.aggregate(0)(_+_,_+_)//可以简写
    println(sum)
  }

  /**
    * 针对值类型aggregate操作的seqOp和combOp相同则可以精简为fold
    * def fold(zeroValue: T)(op: (T, T) => T): T
    */
  def foldTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val max = rdd.fold(0)((v1,v2)=>Math.max(v1,v2))
    println(max)
  }

  /**
    * 输出文件
    * def saveAsTextFile(path: String): Unit
    */
  def saveByTest: Unit ={
    val rdd = sc.makeRDD(1 to 10,1)
    rdd.saveAsTextFile("out")
//    rdd.saveAsObjectFile("path")
    //KV类型的才能保存二进制文件
    //org.apache.spark.rdd.SequenceFileRDDFunctions.saveAsSequenceFile
//    val data = Array(("Fred", 88), ("Tom", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
//    val rdd2 = sc.makeRDD(data)
//    rdd2.saveAsSequenceFile("path")
  }

  /**
    * 针对KV类型 统计每个key的数量
    * def countByKey(): Map[K, Long]
    */
  def countByKeyTest: Unit ={

    val data = Array(("Fred", 88), ("Tom", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val rdd = sc.makeRDD(data)
    val res = rdd.countByKey()
    println(res)
  }

  /**
    * 遍历值类型 执行fun
    * def foreach(f: T => Unit): Unit
    */
  def foreachTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    var sum = 0
    rdd.foreach(sum+=_)
    println(sum)
  }

  /**
    * 针对值类型 统计相关action
    */
  def statisticTest: Unit = {
    val rdd = sc.makeRDD(1 to 100)
    //注意最大最小值有隐式排序
    println(rdd.max())
    println(rdd.min())
    //平均值
    println(rdd.sum())
    println(rdd.count())
    println(rdd.mean())

    //方差
    println(rdd.variance())
    //采样方差
    println(rdd.sampleVariance())
    //标准差
    println(rdd.stdev())
    //采样标准差
    println(rdd.sampleStdev())
  }

  def main(args: Array[String]): Unit = {
//  reduceTest
//    collectTest
//    firstTest
//    takeTest
//    takeSampleTest
//    takeOrderedTest
//    aggregateTest
//    foldTest
//    saveByTest
//    countByKeyTest
    statisticTest
  }
}

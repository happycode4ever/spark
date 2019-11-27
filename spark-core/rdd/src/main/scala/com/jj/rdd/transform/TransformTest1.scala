package com.jj.rdd.transform

import com.jj.rdd.util.PartitionUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag

object TransformTest1 {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform1")
  private val sc = new SparkContext(sparkConf)

  //map针对每个数据进行映射
  //def map[U: ClassTag](f: T => U): RDD[U]
  def mapTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val newRdd = rdd.map(_*2)
    val res = newRdd.collect()
    println(res.mkString(","))
    sc.stop()
  }
  //filter需要一个返回boolean的函数
//  def filter(f: T => Boolean): RDD[T]
  def filterTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val newRdd = rdd.filter(_%3==0)
    val res = newRdd.collect()
    println(res.mkString(","))
    sc.stop()
  }

  //flatMap对于map映射后的集合进行扁平化，所以要求映射是一对多返回
  //def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
  def flatMapTest: Unit ={
    val rdd = sc.makeRDD(1 to 5)
    //返回结构是
    //1
    //1 2
    //1 2 3
    //...
    val newRdd = rdd.flatMap(1 to _)
    //注意返回的是Array[Int] 如果是map会返回Array[Array[Int]]
    val res = newRdd.collect()
    println(res.mkString(","))
    sc.stop()
  }
  //针对一个分区的数据用迭代器映射函数执行，减少函数的调用次数
  //def mapPartitions[U: ClassTag](
  //      f: Iterator[T] => Iterator[U],
  //      preservesPartitioning: Boolean = false): RDD[U]
  def mapPartitionsTest: Unit ={
    //功能是筛选出sex是female的元组筛选出姓名返回
    val data = List(("Mary","female"),("Jack","male"),("Tom","male"),("Sunny","female"))

    val rdd = sc.makeRDD(data)
    val newRdd = rdd.mapPartitions(items=>{
      //元组的筛选只能用匹配模式，直接(k,v)匹配会报错
      items.filter({case (_,v) => v.equalsIgnoreCase("female")}).map(_._1)
    })
    println(newRdd.collect().mkString(","))
    sc.stop()
  }
  //和mapPartitons函数作用相同，就是入参可以获取分区号
  //def mapPartitionsWithIndex[U: ClassTag](
  //      f: (Int, Iterator[T]) => Iterator[U],
  //      preservesPartitioning: Boolean = false): RDD[U]
  def mapPartitionsWithIndexTest: Unit ={
    val data = List(("Mary","female"),("Jack","male"),("Tom","male"),("Sunny","female"))
    val rdd = sc.makeRDD(data)
    val newRdd = rdd.mapPartitionsWithIndex((index, items) => {
//      println(s"$index:${items.mkString(",")}")
      //还是匹配性别是female的元组并且将他在List的索引加上返回
      items.filter({case (_,sex) => sex.equalsIgnoreCase("female")}).map(x => "["+data.indexOf(x)+"]"+x._1)
    })
    //如果没有action操作 计算不会执行
    println(newRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * 打印rdd各分区的数据
    * @param rdd
    * @tparam T 注意什么时候需要加ClassTag
    * @return
    */
  def printRDDPartitionData[T:ClassTag](rdd:RDD[T])={
    val newRdd = rdd.mapPartitionsWithIndex((index,items)=>{
      println(s"$index:["+items.mkString(",")+"]")
      items
    },true)
    newRdd.collect()
  }

  //抽样函数 注意参数如果一样，多次结果也是一样
  //用于大数据集看抽样的数据分布
  //def sample(
  //      withReplacement: Boolean, 是否放回
  //      fraction: Double, 抽样比率
  //      seed: Long = Utils.random.nextLong): RDD[T] 种子 new Random(seed).nextLong 如果是同一个种子产生的随机序列会相同
  def sampleTest: Unit ={
    val rdd = sc.makeRDD(1 to 100)
//   for (i <- 1 to 5){
     val newRdd = rdd.sample(false,0.1)
     println(newRdd.collect().mkString(","))
//    val newRdd2 = rdd.sample(false,0.1,50)
//    println(newRdd2.collect().mkString(","))
//   }
    sc.stop()
  }

  //联合两个同类型的RDD
  //def union(other: RDD[T]): RDD[T]
  def unionTest: Unit ={
    val rdd = sc.makeRDD(1 to 5)
    val rdd2 = sc.makeRDD(1 to 10)
    val resRdd = rdd.union(rdd2)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }
  //求两个Rdd的交集
  //def intersection(other: RDD[T]): RDD[T]
  def intersectionTest: Unit ={
    val rdd1 = sc.makeRDD(1 to 5)
    val rdd2 = sc.makeRDD(3 to 10)
    val resRdd = rdd1.intersection(rdd2)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  //去重操作会打散顺序
  //def distinct(): RDD[T]
  def distinctTest: Unit ={
    val rdd = sc.makeRDD(List(1,1,8,2,5,3,4,5,5,7,8,7,9))
    val resRdd = rdd.distinct()
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  //重新分区这个是属于PairRDDFunction.scala 针对key进行分区例如哈希，同一个key的就会分到一个区
  //def partitionBy(partitioner: Partitioner): RDD[(K, V)]
  def partitionByTest: Unit ={
//    val rdd = sc.makeRDD(1 to 100,5)
//    printRDDPartitionData(rdd)
//    val resRdd = rdd.map(x=>(x,x)).partitionBy(new HashPartitioner(5))
//    printRDDPartitionData(resRdd)
    val rdd = sc.makeRDD(List(("Mary",20),("Tom",15),("Jack",30),("Mary",13),("Sam",15),("Tom",11)))
    val resRdd = rdd.partitionBy(new HashPartitioner(3))
    printRDDPartitionData(resRdd)
  }

  //用于值类型RDD
  //定义初值，区内操作，合并区间操作
  // 坑：初值会参与seqOp与combOp作为最左边的入参进行运算
  //def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
  def aggregateTest: Unit ={
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)
    PartitionUtil.printRdd(rdd)
    val res = rdd.aggregate(10)(math.max(_,_),_+_)
    println(res)
  }
  def aggregateTest2: Unit ={
    val rdd = sc.makeRDD(List("a","b","c","d","e","f"),3)
    PartitionUtil.printRdd(rdd)
    val res = rdd.aggregate("*")({(v1,v2) => if(v1 > v2)v1 else v2},_+_)
    println(res)
  }

  //笛卡尔积测试，慎用，容易OOM
  def cartesianTest: Unit ={
    val genresRDD = sc.makeRDD(Array("adventure","action","comedy"))
    val moviesRDD = sc.makeRDD(Array((1,"adventure|comedy"),(2,"action|comedy"),(3,"adventure|action")))
    val resRDD = genresRDD.cartesian(moviesRDD)
  }

  def main(args: Array[String]): Unit = {
//    mapTest
//    filterTest
//    flatMapTest
//    mapPartitionsTest
//    mapPartitionsWithIndexTest
//    sampleTest
//    unionTest
//intersectionTest
//    distinctTest
//    partitionByTest
//    aggregateTest
    aggregateTest2
  }
}

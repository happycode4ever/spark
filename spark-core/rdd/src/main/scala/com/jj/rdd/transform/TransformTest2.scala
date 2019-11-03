package com.jj.rdd.transform

import com.jj.rdd.util.PartitionUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * 所有ByKey函数都是对应KV类型PairRDDFunctions[K, V]
  */
object TransformTest2 {
  private val sparkConf = new SparkConf().setAppName("transform2").setMaster("local[*]")
  private val sc = new SparkContext(sparkConf)

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

  /**
    * 对key进行预聚合 byKey函数都是已经将key分为一组了，这里需要两个同key的value进行自定义的函数操作
    * def reduceByKey(func: (V, V) => V): RDD[(K, V)]
    */
  def reduceByKeyTest: Unit = {
    val data = List(("Mary", 20), ("Tom", 15), ("Jack", 30), ("Mary", 13), ("Sam", 15), ("Tom", 11))
    val rdd = sc.makeRDD(data)
    //例如求最大值可以简写
    val resRdd = rdd.reduceByKey((v1, v2) => Math.max(v1, v2))
    //    val resRdd = rdd.reduceByKey(Math.max(_,_))
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * 针对值类型自定义分组
    *  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
    *
    */
  def groupByTest: Unit ={
    //样例类姓名年龄
    case class Person(name:String,age:Int)
    val data = List(Person("aa",1),Person("aa",2),Person("bb",1),Person("bb",1),Person("bb",2),Person("cc",1),Person("dd",1))
    //构建键值类型(Person,count)
    val rdd = sc.makeRDD(data).map((_,1))
    //直接根据key分组不能达到需求
    val rdd2 = rdd.groupByKey()
    println(rdd2.collect().mkString(","))

    //需求需要按照姓名分区取到年龄最大的对象并且保留count
    //自定义分组字段为Person的name，这个name会作为新的key,注意分组操作会引起shuffle，这个key会乱序
    val rdd3 = rdd.groupBy({case (Person(name,age),count)=>name})
    println(rdd3.collect().mkString(","))
    //定义隐式组内排序规则
    implicit val myorder = new Ordering[(Person,Int)]{
      override def compare(x: (Person, Int), y: (Person, Int)): Int = y._1.age - x._1.age
    }
    //抛弃原先多余的key,提取每组内容对组内scala排序，取第一条,最后对整段数据按姓名降序排序
    val rdd4 = rdd3.map(_._2.toList.sorted.head).sortBy(_._1.name,false)
    println(rdd4.collect().mkString(","))
    sc.stop()
  }

  /**
    * 只对key进行聚合，不做任何业务操作
    * def groupByKey(): RDD[(K, Iterable[V])] 实际返回的是CompactBuffer类型
    * 返回的迭代器包含这个key聚合的所有数据，不同的key对应的个数可能不一致
    */
  def groupByKeyTest: Unit = {
    val data = List(("Mary", 20), ("Tom", 15), ("Jack", 30), ("Mary", 13), ("Sam", 15), ("Tom", 11))
    val rdd = sc.makeRDD(data)
    val resRdd = rdd.groupByKey()
    println(resRdd.collect().mkString(","))//(Sam,CompactBuffer(15)),(Mary,CompactBuffer(20, 13)),(Tom,CompactBuffer(15, 11)),(Jack,CompactBuffer(30))
    sc.stop()

  }

  /**
    * wordcount通过后置聚合的方式实现
    */
  def groupByKeyTest2: Unit = {
    val words = Array("one", "two", "two", "three", "three", "three")
    val rdd = sc.makeRDD(words).map((_, 1))
    val groupRdd = rdd.groupByKey()
    val resRdd = groupRdd.map({ case (word, items) => (word, items.sum) })
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    *  KV类型的RDD之间对于相同key的value进行聚合，不同rdd的结果分别以迭代器包含，最终以CompactBuffer类型返回
    *  如果对应key的rdd没有值返回空的CompactBuffer() 例如(4,(CompactBuffer(d),CompactBuffer()))
    *  重载的方法最多接受3个rdd
    * def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
    */
  def cogroupTest: Unit ={
    val data1 = Array((1,"a"),(2,"b"),(3,"c"),(4,"d"))
    val data2 = Array((1,4),(2,5),(3,6),(5,7))
    val rdd1 = sc.makeRDD(data1)
    val rdd2 = sc.makeRDD(data2)
    val resRdd = rdd1.cogroup(rdd2)
    println(resRdd.collect().mkString(","))//(1,(CompactBuffer(a),CompactBuffer(4))),(2,(CompactBuffer(b),CompactBuffer(5))),(3,(CompactBuffer(c),CompactBuffer(6))),(4,(CompactBuffer(d),CompactBuffer())),(5,(CompactBuffer(),CompactBuffer(7)))
    sc.stop()
  }

  /**
    * def combineByKey[C](  已经将同个key分为一组，现在需要根据业务定义返回值类型
    * createCombiner: V => C, 定义返回值类型的初始值 同分区的操作1：这个函数是第一次出现这个key进行的操作，将这个键的值V初始化为C结构的初始值
    * mergeValue: (C, V) => C, 同分区的操作2：这个函数是再次遇到这个key需要进行的操作，C是上一次运算产生的结果，V是当前这个key对应的值，需要走这个函数构建为C的结构
    * mergeCombiners: (C, C) => C): RDD[(K, C)] 不同的分区的操作：这个函数是不同分区同个key运算的结果C1和C2，走当前的函数输出最终这个key的结果
    */
  def combineByKeyTest: Unit = {
    val data = Array(("Fred", 88), ("Tom", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val rdd = sc.makeRDD(data)
    //求平均分需要总分与数量,定义初始值结构为(score,1) _1是总分，_2是数量 第一次进来需要记录，不然就丢失这一次的数据了
    val resRdd = rdd.combineByKey(score => (score, 1) //这个是当前key也就是name，对应的值score 需要初始化为(0,0)
          , (result: (Int, Int), score) => (result._1 + score, result._2 + 1) //再次遇见这个key对应的操作，叠加上次的分数result._1+score，并且上次的次数+1 result._2+1
          , (result1: (Int, Int), result2: (Int, Int)) => (result1._1 + result2._1, result1._2 + result2._2)) //最终合并分区的操作 叠加各自的分数与次数
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * combineByKey简化版
    * def aggregateByKey[U: ClassTag](zeroValue: U)直接定义初始值
    * (seqOp: (U, V) => U, 同个分区内的操作
    * combOp: (U, U) => U): RDD[(K, U)] 不同分区的操作
    */
  def aggregateByKeyTest: Unit = {
    val data = Array(("Fred", 88), ("Tom", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val rdd = sc.makeRDD(data,3)
    PartitionUtil.printRdd(rdd)
    val resRdd = rdd.aggregateByKey((0, 0))(//定义初始值(0,0)
      (result: (Int, Int), score) => (result._1 + score, result._2 + 1)//每个分区内的分数汇总
      , (result1: (Int, Int), result2: (Int, Int)) => (result1._1 + result2._1, result1._2 + result2._2))//多个分区内的汇总
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  def aggregateByKeyTest2: Unit ={
    val data = Array(("Tom", 1), ("Tom", 1), ("Tom", 1), ("Tom", 1), ("Tom", 1), ("Tom", 1))
    sc.makeRDD(data,3).aggregateByKey(10)(_+_,_+_).collect().foreach(println)
  }

  /** aggregate的简化 用于seqOp和combOp是同一个操作的场景，例如求最大值
    * //注意初始值是V必须是源数据对应的值类型
    * def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
    */
  def foldByKeyTest: Unit ={
    val data = Array(("Fred", 88), ("Tom", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val rdd = sc.makeRDD(data)
    val resRdd = rdd.foldByKey(0)((score1,score2)=>Math.max(score1,score2))
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * 注意key必须实现Ordering
    * def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
    * : RDD[(K, V)]
    */
  def sortByKeyTest: Unit ={
    val data = Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd"))
    val rdd = sc.makeRDD(data)
//    rdd.sortByKey(true)
    val resRdd = rdd.sortByKey(false)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * 注意sortByKey源码里
    * new ShuffledRDD[K, V, V](self, part)
    * .setKeyOrdering(if (ascending) ordering else ordering.reverse)
    * 调用了ordering是本类的private val ordering = implicitly[Ordering[K]]
    * 所以如果自定义类也要支持sortByKey需要隐式声明一个变量Ordering[T] 实现compare方法支持排序
    */
  def sortByKeyTest2: Unit ={
    case class Person(name:String)
    implicit val myorder = new Ordering[Person] {
      override def compare(x: Person, y: Person): Int = x.name.compareTo(y.name)
    }
    val data = Array((Person("Peter"),60),(Person("Mary"),40),(Person("John"),70),(Person("Peter"),80),(Person("Mary"),60))
    val rdd = sc.makeRDD(data)
    val resRdd = rdd.sortByKey(false)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }


  /**
    * sortBy针对值类型,或者也可以把键值类型当成元组类型
    * def sortBy[K](
    * f: (T) => K, 根据函数的返回值排序
    * ascending: Boolean = true,指定升序降序
    * numPartitions: Int = this.partitions.length)
    * (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    */
  def sortByTest: Unit ={
    val rdd = sc.makeRDD(1 to 10)
    val resRdd = rdd.sortBy(x => x % 3)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  def sortByTest2: Unit ={
    case class Person(name:String,age:Int)
    //如果需要自定义排序还是要定义隐式变量实现排序方法
    implicit val myorder = new Ordering[Person] {
      override def compare(x: Person, y: Person): Int = {
        val i = x.name.compareTo(y.name)
        if(i!=0)i
        else x.age.compareTo(y.age)
      }
    }
    val data = Array(Person("Peter",20),Person("Mary",15),Person("Tom",30),Person("Peter",10),Person("Tom",15))
    val rdd = sc.makeRDD(data)
    //排序时直接返回本对象即可
//    val resRdd = rdd.sortBy(x=>x)
    //如果只是对象的部门字段排序可以直接提取值作为排序条件
    val resRdd = rdd.sortBy(x=>x.name)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  def sortByTest3: Unit ={
    val rdd = sc.makeRDD(List(("Tom",10),("Mary",20),("Jerry",30),("Jim",20)))
    rdd.sortBy(_._2).collect().foreach(println)
  }

  /**
    * join针对键值类型连接两个RDD，注意key值类型必须相同，然后将同个key的不同value放在一个元组里
    * PairRDDFunctions[K, V]
    * def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
    * def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
    * def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))]
    */
  def joinTest: Unit ={
    val data1 = Array((1,"a"),(2,"b"),(3,"c"),(4,"d"))
    val data2 = Array((1,4),(2,5),(3,6),(5,7))
    val rdd1 = sc.makeRDD(data1)
    val rdd2 = sc.makeRDD(data2)
    val resRdd = rdd1.join(rdd2)
    println(resRdd.collect().mkString(","))//(1,(a,4)),(2,(b,5)),(3,(c,6)) 只会将相同key的v放一块
    //leftJoin 合并的元组里对于rdd1的数据直接返回，而rdd2有的数据返回some，没有的返回none
    val resRdd2 = rdd1.leftOuterJoin(rdd2)
    println(resRdd2.collect().mkString(","))//(1,(a,Some(4))),(2,(b,Some(5))),(3,(c,Some(6))),(4,(d,None))
    //同理rightJoin 则是rdd2一定返回，对应rdd1有的是Some，没有的是None
    val resRdd3 = rdd1.rightOuterJoin(rdd2)//(1,(Some(a),4)),(2,(Some(b),5)),(3,(Some(c),6)),(5,(None,7))
    println(resRdd3.collect().mkString(","))
    sc.stop()
  }

  /**
    * 笛卡尔积操作，永远不要用,会返回m*n的数据
    * def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)]
    */
  def cartesianTest: Unit ={
    val rdd1 = sc.makeRDD(1 to 3)
    val rdd2 = sc.makeRDD(2 to 5)
    val resRdd = rdd1.cartesian(rdd2)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * TODO spark可以执行外部脚本例如Linux命令
    */
  def pipeTest: Unit ={
//    val rdd = sc.makeRDD(List("hi","Hello","how","are","you"),2)
//    val path = ""
//    val command = ""
//    //可以利用pipe函数调用外部脚本 可以是路径或者直接脚本命令
//    // 注意脚本必须能找得到 如果是集群模式必须每个节点都有这个脚本
//    //这个脚本会针对每个分区执行一次
//    val resRdd = rdd.pipe(path)
//    println(resRdd.collect().mkString(","))
    //[aaa@hadoop113 mysh]$ vim spark-pipe.sh
    //
    //pipe.sh:
    //#!/bin/sh
    //echo "AA"
    //while read LINE; do
    //   echo ">>>"${LINE}
    //done
  }

  /**
    * coalesce 英 [ˌkəʊəˈles] 美 [ˌkoʊəˈles] v. 合并; 联合; 结合;
    * 缩减分区数，用于大数据集过滤后，提高小数据集的执行效率,默认没有进行shuffle操作
    * def coalesce(numPartitions: Int, shuffle: Boolean = false,
    * partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
    * (implicit ord: Ordering[T] = null)
    */
  def coalesceTest: Unit ={
    //例如1亿条数据100个分区，做了filter操作,每个分区大概filter掉70%数据,这个时候一个分区只剩下30万数据,这时候缩减分区，相当于把几个分区的数据合并到一块
    val rdd = sc.makeRDD(1 to 100,6)
    printRDDPartitionData(rdd)
    val rdd2 = rdd.coalesce(3)
    printRDDPartitionData(rdd2)
  }

  /**
    * 重新分区，带shuffle操作
    * def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    * coalesce(numPartitions, shuffle = true)
    * }
    */
  def repartitionTest: Unit ={
    val rdd = sc.makeRDD(1 to 100,6)
    printRDDPartitionData(rdd)
    val rdd2 = rdd.repartition(3)
    printRDDPartitionData(rdd2)
  }

  /**
    * 适应于KV类型的Rdd
    * org.apache.spark.rdd.OrderedRDDFunctions#repartitionAndSortWithinPartitions(org.apache.spark.Partitioner)
    * def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)]
    * repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
    */
  def repartitionWithinSortTest: Unit ={
    val rdd1 = sc.makeRDD(1 to 100,6).map(x => (x,x))
    printRDDPartitionData(rdd1)
    println("----------------------------------")
    val rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    printRDDPartitionData(rdd2)
  }

  /**
    * 将每个分区数据聚合成Array[T]
    * def glom(): RDD[Array[T]]
    * 最终collect返回的类型是Array[Array[T]]
    */
  def glomTest: Unit ={
    val rdd = sc.makeRDD(1 to 20,5)
    printRDDPartitionData(rdd)
    println("-------------------------")
    val resRdd = rdd.glom()
    printRDDPartitionData(resRdd)
    println("-------------------------")
    val array = resRdd.collect()
    array.foreach(x=>println(x.mkString(",")))
    sc.stop()
  }

  /**
    * 针对键值类型的RDD，只处理Value
    * def mapValues[U](f: V => U): RDD[(K, U)]
    */
  def mapValuesTest: Unit ={
    val rdd = sc.parallelize(Array((1,"abc"),(1,"dcba"),(2,"b"),(3,"c")))
    //对每个value的字符串实现每个字符+1
    val resRdd = rdd.mapValues(_.toCharArray.map(x=>(x+1).toChar).mkString)
    println(resRdd.collect().mkString(","))
    sc.stop()
  }

  /**
    * 数值型rdd的减法操作, 也就是除掉交集部分
    * def subtract(other: RDD[T]): RDD[T]
    */
  def subtractTest: Unit ={
    val rdd1 = sc.makeRDD(1 to 5)
    val rdd2 = sc.makeRDD(3 to 8)
    val res1 = rdd1.subtract(rdd2)
    println(res1.collect().mkString(","))
    val res2 = rdd2.subtract(rdd1)
    println(res2.collect().mkString(","))
  }

  def main(args: Array[String]): Unit = {
    //    reduceByKeyTest
//        groupByKeyTest
    //    groupByKeyTest2
//    groupByTest
//    cogroupTest
//    combineByKeyTest
//        aggregateByKeyTest
        aggregateByKeyTest2
//    foldByKeyTest
//    sortByKeyTest
//    sortByKeyTest2
//    sortByTest
//    sortByTest2
//    sortByTest3
//    joinTest
//    cartesianTest
//    coalesceTest
//    repartitionTest
//    repartitionWithinSortTest
//    glomTest
//    mapValuesTest
//    subtractTest
  }

}

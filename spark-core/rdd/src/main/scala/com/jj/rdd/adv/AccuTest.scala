package com.jj.rdd.adv

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccuTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
  private val sc = new SparkContext(sparkConf)

  def defaultAccuTest: Unit ={
    val rdd = sc.makeRDD(1 to 100)
    //sc.accumulator已经过时了 可以换
//    sc.longAccumulator
//    sc.doubleAccumulator

    //如果数据量大可以采用公有的一个累加器求和
    //如果我们想实现所有分片处理时更新共享变量的功能，那么累加器可以实现我们想要的效果。
    val accu = sc.accumulator(0)
    rdd.foreach(x=>if(x%3==0)accu += 1)
    println(accu.value)
  }

  def accuProblem: Unit ={
    val accum = sc.longAccumulator("longAccum")
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{
      accum.add(1L)
      n+1
    })
    //累加器是懒执行的，后面的action都会重新计算，那么值就会成倍增加
    numberRDD.count
    //解决这个问题使用缓存即可numberRDD.cache().count()
    println("accum1:"+accum.value)
    numberRDD.reduce(_+_)
    println("accum2: "+accum.value)
  }

  def customAccTest: Unit ={
    val rdd = sc.makeRDD(List("aa","aa","bb","cc","bb","dd","cc","bb"))
    val myAcc = new MyAccumulator
    sc.register(myAcc,"myacc")
    rdd.foreach(myAcc.add(_))
    println(myAcc.value)
  }

  def main(args: Array[String]): Unit = {
//   defaultAccuTest
    customAccTest
  }
}

//AccumulatorV2[INT,OUT]代表输入输出
class MyAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]] {
  private val _hashAcc:mutable.HashMap[String,Int] = mutable.HashMap()
  //判断是否为空
  override def isZero: Boolean = _hashAcc.isEmpty

  //拷贝当前的累加器为一个新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new MyAccumulator
    //**不知道为什么同步
//    _hashAcc.synchronized{
      newAcc._hashAcc.++=(_hashAcc)
//    }
    newAcc
  }

  //重置当前累加器
  override def reset(): Unit = _hashAcc.clear()

  //每一个分区用于添加数据的方法 小的sum
  override def add(key: String): Unit = {
    _hashAcc.get(key) match {
      case Some(v) => _hashAcc += ((key,v+1))
      case None => _hashAcc += ((key,1))
    }
  }

  //合并别的分区进自己的分区的输出 总的sum
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    for((key,count) <- other.value){
      _hashAcc.get(key) match {
        case Some(this_count) => _hashAcc += ((key , count+this_count))
        case None => _hashAcc += (key -> 1)
      }
    }
  }

  //输出值
  override def value: mutable.HashMap[String, Int] = _hashAcc
}

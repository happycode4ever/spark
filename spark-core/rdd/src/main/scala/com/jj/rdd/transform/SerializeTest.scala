package com.jj.rdd.transform

import org.apache.spark.SparkContext

object SerializeTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[4]","SerializeTest")
    val rdd = sc.makeRDD(List(1,3,4,5))
    //map操作里需要用到该对象 就需要序列化
    rdd.map(num => new Num(num)).collect().foreach(x=>println(x.num))
  }
}
//class Num(val num:Int){ //算子进行计算需要用到对象成员或者该对象的话，需要序列化,不然会抛异常
class Num(val num:Int) extends Serializable {
}

package com.jj.rdd.adv

import com.jj.rdd.util.PartitionUtil
import org.apache.spark._

import scala.util.Random

object PartitionTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("cache")
  private val sc = new SparkContext(sparkConf)
  def main(args: Array[String]): Unit = {

    def genKey:String = {
      val a = 'a'
      val random = new Random()
      val seq = for(i<- 1 to 5) yield{
        (random.nextInt(26)+a).toChar
      }
      seq.mkString
    }

    val rdd = sc.makeRDD(1 to 100).map(_ => (genKey,1))
    PartitionUtil.printRdd(rdd)
    println("------------------------")
    //HashPartition使用
    val rdd2 = rdd.partitionBy(new HashPartitioner(4))
    PartitionUtil.printRdd(rdd2)
    println("------------------------")
    //RangePartition使用要求key必须可以排序 分区内不一定有序，分区之间保持有序，而且分区内数据大致保持均匀
    val rdd3 = rdd.partitionBy(new RangePartitioner(4,rdd,false))
    PartitionUtil.printRdd(rdd3)
    println("------------------------")
    //自定义分区
    val rdd4 = rdd.partitionBy(new MyPartitioner(4))
    PartitionUtil.printRdd(rdd4)

  }
}
class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      key.asInstanceOf[Int]&Integer.MAX_VALUE%num
    }else{
      key.toString.hashCode&Integer.MAX_VALUE%num
    }
  }
}

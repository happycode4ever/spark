package com.jj.rdd.exercise

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object RddExercise {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
  private val sc = new SparkContext(sparkConf)

  //timestamp province city userid adid
  //时间戳    省份      城市 用户id  广告id
  //用户id 0-99 省份城市id0-9 adid 0-19
  //需求：1每个省份点击Top3的广告Id
  //2每个省份每个小时top3的广告id
  case class Data(timestamp: Long, province: Int, city: Int, userid: Int, adid: Int)

  def test1(): Unit = {
    val random = new Random()
    val dataList: ListBuffer[(Int, Int)] = ListBuffer()
    val adidList = List(1,2,2,3,3,3,4,4,4,4,5,5,5,5,5)
    for (i <- 1 to 50) {
      val province = random.nextInt(10)
//      val adid = random.nextInt(19)
      //adid 固定 5个5 4个4 3个3 2个2 1个1 其他随机
      val adid = if(i<=15) adidList(i-1) else random.nextInt(19)
      dataList.append((province, adid))
    }
    println(dataList)
    val sourceRdd = sc.makeRDD(dataList)
    //先按城市分组 统计每个城市的广告id次数 对广告id在组内排序 组内取前三

    //1统计省份和广告作为key，出现的次数 结构为((prov,adid),count)
    val rdd1 = sourceRdd.map((_, 1)).reduceByKey(_ + _)
    //2转换key为(prov,(adid,count)) 目的是将省份分组并排序
    val rdd2 = rdd1.map({ case ((pro, adid), count) => (pro, (adid, count)) }).groupByKey().sortByKey()
    //3对于组内的数据采用scala排序筛选出top3的(adid,count)
    val rdd3 = rdd2.flatMap({ case (pro, items) => {
      //4最终输出样式 pro adid count拼接需要的格式可以利用yield生成Vetor，扁平化返回即可
      val topList = items.toList.sortWith((item1, item2) => item1._2 > item2._2).take(3)
      for (item <- topList) yield {
        pro + " " + item._1 + " " + item._2
      }
    }
    })
    rdd3.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    test1()
  }
}

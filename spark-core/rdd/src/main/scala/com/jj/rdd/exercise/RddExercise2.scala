package com.jj.rdd.exercise

import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * IP 命中率 响应时间 请求时间 请求方法 请求URL    请求协议 状态吗 响应大小 referer 用户代理
  * 111.19.97.15 HIT 18 [15/Feb/2017:00:00:39 +0800] "GET http://cdn.v.abc.com.cn/videojs/video-js.css HTTP/1.1" 200 14727 "http://www.zzqbsm.com/" "Mozilla/5.0+(Linux;+Android+5.1;+vivo+X6Plus+D+Build/LMY47I)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/35.0.1916.138+Mobile+Safari/537.36+T7/7.4+baiduboxapp/8.2.5+(Baidu;+P1+5.1)"
  * 1、计算每一个IP的访问次数  (114.55.227.102,9348)
  * 2、计算每一个视频访问的IP数 视频：141081.mp4 独立IP数:2393
  * 3、统计每小时CDN的流量00时 CDN流量=14G
  */
object RddExercise2 {

  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rddExercise2")
  private val sc = new SparkContext(sparkConf)
  //时间解析器避免生成太多对象
  private val formatter = new DateTimeFormatterBuilder().appendLiteral("[").appendPattern("dd/MMM/yyyy:HH:mm:ss").appendLiteral("+0800]").toFormatter(Locale.ENGLISH)


  //  解析数据样例类
  case class Data(ip: String, hit: String, time: Double, reqTime: String, method: String, url: String, protocol: String, responseCode: Int, size: Long, referer: String, agent: String)

  //解析行
  def parseData(line: String): Array[String] = {
    line.replace("\"", "").replace(" +0800", "+0800").split(" ")
  }

  //装载数据
  def loadData: RDD[Data] = {
    val sourceRdd = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-core\\rdd\\src\\main\\resources\\cdn.txt")
    //如果数据过多可以考虑采样
    //    val sampleRdd= sourceRdd.sample(false,0.01)
    //    sampleRdd.saveAsTextFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-core\\rdd\\src\\main\\resources\\sample")
    val filterRdd = sourceRdd.filter(line => {
      val fields = parseData(line)
      if (fields.length < 11) false
      else true
    }).map(line => {
      val fields = parseData(line)
      Data(fields(0), fields(1), fields(2).toDouble, fields(3), fields(4), fields(5), fields(6), fields(7).toInt, fields(8).toLong, fields(9), fields(10))
    })
    filterRdd.cache()
    filterRdd.collect()
    filterRdd
  }

  //1、计算每一个IP的访问次数  (114.55.227.102,9348)
  def test1(rdd: RDD[Data]): Unit = {
    //分区内采用scala预聚合
    val sumRdd1 = rdd.mapPartitions(items => {
      val hashmap = mutable.HashMap[String, Long]()
      while (items.hasNext) {
        val ip = items.next().ip
        hashmap.get(ip) match {
          case Some(count) => hashmap += (ip -> (count + 1))
          case None => hashmap += (ip -> 1)
        }
      }
      hashmap.toIterator
    })
    val sumRdd2 = sumRdd1.reduceByKey(_ + _)
    sumRdd2.collect()
//      .foreach(println)
  }
  def test11(rdd:RDD[Data]): Unit ={
    //原生方式mapreduce
    val sumRdd1 = rdd.map(data=>(data.ip,1))
    val sumRdd2 = sumRdd1.reduceByKey(_+_)
    sumRdd2.collect()
//      .foreach(println)
  }

  def test111(rdd:RDD[Data]): Unit ={
    //map+预聚合的方式
    val sumRdd1 = rdd.map(data=>(data.ip,1))
    val sumRdd2 = sumRdd1.foldByKey(0)(_+_)
    sumRdd2.collect()
//      .foreach(println)
  }

  //2、计算每一个视频访问的IP数 视频：141081.mp4 独立IP数:2393
  def test2(rdd:RDD[Data]): Unit ={
    def dataFormat(url:String,count:Long) = "视频："+url+" 独立IP数:"+count

//    val resRdd = rdd.filter(data=>data.url.endsWith(".mp4")).map(data=>(data.url+"_"+data.ip,1)).reduceByKey(_+_)
//    resRdd.collect().foreach(println)
    //筛选访问url为.mp4结尾的数据，重组数据为(url,ip)
    val rdd1 = rdd.filter(data=>data.url.endsWith(".mp4")).map(data=>(data.url,data.ip))
    //按照url分组
    val rdd2 = rdd1.groupByKey().map({case (url,ips)=>{
      //组内ip去重
     val count = ips.toList.distinct.size
      //最后格式化输出需要的数据
      dataFormat(url,count)
    }})
    rdd2.collect().foreach(println)
  }

  def formatReqTime(reqTime:String)={
    //ofPattern不认[]这个符号
//    val formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")
//    val formatter = new DateTimeFormatterBuilder().appendLiteral("[").appendPattern("dd/MMM/yyyy:HH:mm:ss").appendLiteral("+0800]").toFormatter(Locale.ENGLISH)
    val time = LocalDateTime.parse(reqTime,formatter)
    time
  }

  def matchTime(reqTime:String)={
    val pattern = "\\[\\d{2}/[a-zA-Z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\+0800\\]"
    reqTime.matches(pattern)
  }

  //3、统计每小时CDN的流量00时 CDN流量=14G
  def test3(rdd:RDD[Data]): Unit ={
    //筛选时间正则匹配的，另外不是404请求的数据
    val rdd1 = rdd.filter(data=>matchTime(data.reqTime)&&data.responseCode==200)

    val rdd2 = rdd1.map(data=>{
      val time = formatReqTime(data.reqTime)
      (time.getHour,data.size)
    }).reduceByKey(_+_).sortByKey()
    rdd2.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
      val dataRdd = loadData
    test3(dataRdd)

  }
}

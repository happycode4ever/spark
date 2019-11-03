package com.jj.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object WordCount {
  private val logger: Logger = LoggerFactory.getLogger(WordCount.getClass)

  def wc(args:Array[String]): Unit ={
//    val from = args(0)
//    val to = args(1)
    val from = "LICENSE"
    val to = "result"

    //setMaster使用local[*]用于本地测试 不需要spark集群
    //    val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")

    //测试集群模式 不常用 目前本地文件会找不到 HDFS路径则因为用户名被拦截
    //    val sparkConf = new SparkConf().setAppName("wc").setMaster("spark://hadoop114:7077")
    //  .setJars(List("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-core\\wordcount\\target\\wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar"))
    //  .setIfMissing("spark.driver.host","192.168.1.1")

    val sparkConf = new SparkConf().setAppName("wc")

    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(from)
    val resultMap1 = rdd.flatMap(x => x.split(" "))
    val resultMap2 = resultMap1.map(x => (x, 1))
    val reduceResult = resultMap2.reduceByKey((x, y) => x + y)
    logger.info("reduce Result is : {}", reduceResult.collect())
    reduceResult.saveAsTextFile(to)

  }

  def wc2: Unit ={
    val sparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("LICENSE")
    println(rdd.collect().mkString(","))
  }

  def main(args: Array[String]): Unit = {
    wc(args)
//    wc2
  }
}

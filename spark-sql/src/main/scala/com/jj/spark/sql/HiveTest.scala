package com.jj.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object HiveTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hivetest")
  private val spark = SparkSession.builder()
    .config(sparkConf)
    //开启hive支持 spark-shell/spark-sql默认集成了hive不需要设置
    //如果需要IDEA本地跑还需要spark-hive的依赖以及hive-site的配置
    .enableHiveSupport()
    .getOrCreate()
  private val sc = spark.sparkContext
  private val logger = LoggerFactory.getLogger(HiveTest.getClass)
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val df = spark.sql("select * from spark.src")
    val data = df.collect().mkString(",")
    logger.info("df:{}",data)

//    df.write.mode("append")
//      .format("jdbc")
//      .option("url","jdbc:mysql://hadoop112:3306/rdd")
//      .option("dbtable","kv")
//      .option("user","root")
//      .option("password","123456").save()


    spark.stop()
  }
}

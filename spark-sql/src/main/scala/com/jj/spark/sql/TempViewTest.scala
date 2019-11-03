package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}

object TempViewTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("tempview")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._
    val df = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\people.json")
    df.agg(col("name"))
    //session作用域内直接访问表名，session结束自动删除这张临表
    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    //应用内可访问，需要带上global_temp.前缀，应用结束自动删除临表
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()

//    spark.sql("SELECT (8  + (CAST (RAND() * 50000 AS decimal(7,2))) * 12 )").show()
    //    spark.sql("SELECT (8  + (CAST (RAND() * 50000 AS decimal(7,2))) * 12 )").show()
//    spark.sql("SELECT (8  + (CAST (RAND() * 50000 AS bigint)) * 12 )").show()
  }
}

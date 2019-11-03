package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("readtest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    //sparksql 读取的text是把整行数据当做一个列，数据类型是String
//    root
//    |-- value: string (nullable = true)

    val df = spark.read.text("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\people2.txt")
    df.printSchema()
    df.show()
    val df2 = spark.read.textFile("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\people2.txt")
    df2.printSchema()
    df2.show()
  }
}

package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UDFTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UDFTest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext

  def UDFTest: Unit ={
    //自定义UDF 一个输入一个输出，直接定义匿名函数即可，注意入参要指定类型并且带括号
    spark.udf.register("add",(x:String)=>"name:"+x)
    val df = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\people.json")
    df.createOrReplaceTempView("people")

    spark.sql("select add(name) from people").show()
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    UDFTest
  }
}

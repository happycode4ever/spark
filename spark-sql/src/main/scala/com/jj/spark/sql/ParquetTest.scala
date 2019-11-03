package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ParquetTest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\employees.json")
    df.write.mode(SaveMode.Overwrite).parquet("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\employees.parquet")
    val df2 = spark.read.format("parquet").load("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\employees.parquet")
    df2.show()
  }
}

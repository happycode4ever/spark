package com.jj.spark.adv

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object PivotTest {
  case class Temp(date:String,temp:Int)
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("PivotTest")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //装载天气数据 2019-09-10 75 代表日期和温度
    val rdd= sc.textFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\adv\\temp.txt").map(line => {
      val attrs = line.split(" ")
      Row(attrs(0),attrs(1).toInt)
    })
    val schema = StructType(StructField("date",DataTypes.StringType)::StructField("temp",DataTypes.IntegerType)::Nil)
    val df = spark.createDataFrame(rdd,schema)
    df.show()
    //行转列操作使用pivot
    import sql.functions._
    df.createOrReplaceTempView("t1")

    /*
      思路：
      1.先转换字符串为时间，通过时间函数提取年月，变更数据格式为2019 9 75
      2.通过pivot透视行转列，按照年份聚合，行转列字段是月份，然后对每个月的气温取平均值
     */

    //sql语句写法使用pivot(聚合函数 for 需要转换的列 in (转换列相关的值))
    //坑：2.4版本以上的sparksql才支持，所以一般使用DSL形式
    //还有sql方式必须外层再套一个select * from
//    spark.sql("select * from (select year(date) year,month(date) month,temp from " +
//      "(select to_date(date) date,temp from t1)) t2 " +
//        "pivot(avg(temp) for month in(1,2,3,4,5))").show()
    //DSL形式使用pivot .groupBy(分组字段).pivot(转换列).agg(聚合函数)
    spark.sql("select year(date) year,month(date) month,temp from " +
          "(select to_date(date) date,temp from t1)")
          .groupBy("year").pivot("month").agg(avg("temp")).show()
  }
}

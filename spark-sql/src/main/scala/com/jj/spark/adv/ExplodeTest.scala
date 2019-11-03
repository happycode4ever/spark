package com.jj.spark.adv

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ExplodeTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ExplodeTest")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val rdd = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\adv\\explode_movie.txt")
      .map(line => {
        val attrs = line.split(" "); Row(attrs(0), attrs(1))
      })
    val schema = StructType(StructField("mid",DataTypes.StringType)::StructField("genres",DataTypes.StringType)::Nil)
    val df = spark.createDataFrame(rdd,schema)
    df.createOrReplaceTempView("t")
    //思路：
    //1.先将类别列按照|字符切分成数组(注意坑点，需要用\\\\来转义特殊字符)
    //2.炸开这个数组列
    //3.作为侧写视图的字段添加到该表
    spark.sql("select mid,g from t lateral view explode(split(genres,'\\\\|')) as g").show()
  }
}

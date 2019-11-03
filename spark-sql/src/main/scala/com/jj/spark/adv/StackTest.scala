package com.jj.spark.adv

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object StackTest {
  case class Score(subject:String,s1:Int,s2:Int,s3:Int,s4:Int)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StackTest").setMaster("local[2]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val title = "科目 李怡 李毅 李义 李奕"
    val ts = title.split(" ")
    val df = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\adv\\stack_score.txt")
      .map(line => {val attrs = line.split(" ");Score(attrs(0),attrs(1).toInt,attrs(2).toInt,attrs(3).toInt,attrs(4).toInt)})
      .toDF()
    //    println(title)
    //    rdd.foreach(println)
    val sourceDf = df.select($"subject".as(ts(0)), $"s1".as(ts(1)), $"s2".as(ts(2)), $"s3".as(ts(3)), $"s4".as(ts(4)))
    sourceDf.show()
    sourceDf.createOrReplaceTempView("t")
    spark.sql("select `科目`,stack(3,'李怡', `李怡`, '李毅', `李毅`, '李义', `李义`) as (`姓名`,`分数`) from t").show()

  }
}

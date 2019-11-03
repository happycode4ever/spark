package com.jj.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object JDBCTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("jdbctest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  import spark.implicits._

  def readJDBCTest: Unit ={
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    val df1 = spark.read.jdbc("jdbc:mysql://hadoop112:3306/rdd","kv",prop)
    df1.show()

    val df2 = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop112:3306/rdd")
      .option("dbtable", " rddtable")
      .option("user", "root")
      .option("password", "123456").load()
    df2.show()
    spark.stop()
  }

  def writeJDBCTest: Unit ={
    val rdd = sc.makeRDD(List((12,"12a","12a"),(13,"13a","13a"),(14,"14a","14a")))
    val df = rdd.toDF("id","name","info")
    println(df.collect().mkString(","))
    val prop = new Properties()
      prop.put("user", "root")
      prop.put("password", "123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop112:3306/rdd","rddtable",prop)
  }
  def writeJDBCTest2: Unit ={
    val rdd = sc.makeRDD(List(("12a",12),("13a",13),("14a",14)))
    val df = rdd.toDF("key","value")
    println(df.collect().mkString(","))
    val prop = new Properties()
      prop.put("user", "root")
      prop.put("password", "123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop112:3306/rdd","kv",prop)
  }
  def writeJDBCTest3: Unit ={
    val rdd = sc.makeRDD(List(("12a",12),("13a",13),("14a",14)))
    val df = rdd.toDF("key","value")
    println(df.collect().mkString(","))
    //记得用format的形式最后要加动作save还是load
    df.write.mode(SaveMode.Append).format("jdbc")
      .option("url", "jdbc:mysql://hadoop112:3306/rdd")
      .option("dbtable", " kv")
      .option("user", "root")
      .option("password", "123456").save()
  }

  def main(args: Array[String]): Unit = {
//    writeJDBCTest3
    readJDBCTest
  }
}

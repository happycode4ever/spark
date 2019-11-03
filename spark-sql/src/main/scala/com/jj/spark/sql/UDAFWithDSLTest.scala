package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.DataTypes

object UDAFWithDSLTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UDAFWithDSLTest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    UDAFWithDSL
  }
  def UDAFWithDSL: Unit ={
    val df = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\employees.json")
    val df2 = df.withColumn("salary",$"salary".cast(DataTypes.IntegerType))
//    df2.printSchema()
    val ds = df2.as[Input]
//    ds.printSchema()
//    ds.show()
    val average = new MyAverageWithDSL().toColumn.name("average")
    ds.select(average).show()
  }

}
//**坑就是df2.as[Input]转换DS的类型必须是Aggregator[Input,Buffer,Double]的Input同类型，也就是说输入必须是整行数据，这个存在局限
//第二点必须采用DSL方式将这个自定义的聚合函数转换为一个列
//case class Emp(name:String,salary:Long)
case class Input(name:String,salary:Integer)
//注意转换结构的入参要可变
case class Buffer(var sum:Long,var count:Integer)
class MyAverageWithDSL extends Aggregator[Input,Buffer,Double] {
  override def zero: Buffer = Buffer(0L,0)

  override def reduce(b: Buffer, a: Input): Buffer = {
    b.sum +=  a.salary
    b.count += 1
    b
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Buffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

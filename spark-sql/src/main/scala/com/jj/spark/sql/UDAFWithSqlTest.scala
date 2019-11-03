package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
  * {"name":"Michael", "salary":3000}
  * {"name":"Andy", "salary":4500}
  * {"name":"Justin", "salary":3500}
  * {"name":"Berta", "salary":4000}
  * 计算平均工资
  */
class MyAverageWithSql extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("salary",DataTypes.IntegerType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("sum",DataTypes.LongType)::StructField("salary",DataTypes.IntegerType)::Nil)

  override def dataType: DataType = DataTypes.DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0)+input.getInt(0)
    buffer(1)=buffer.getInt(1)+1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1) = buffer1.getInt(1)+buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }
}

object UDAFWithSqlTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UDAFWithSqlTest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  import spark.implicits._

  def UDAFWithSql: Unit ={
    val df = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\employees.json")
    df.createOrReplaceTempView("emp")
    //缓存中间表，用于后面多次操作,也是懒加载的，第一次执行action触发
    spark.sqlContext.cacheTable("emp")
    df.withColumn("salary",$"salary".cast(DataTypes.IntegerType))
    //注意注册的UDAF对象不能嵌套三层内部类，最好放在外部
    spark.udf.register("average",new MyAverageWithSql)
    val frame = spark.sql("select average(salary) avg from emp")
    println(frame.collect().map(_.getDouble(0)).head)
    frame.show()
    //中间表使用完毕需要清除缓存
    spark.sqlContext.uncacheTable("emp")
    //清除所有缓存
//    spark.sqlContext.clearCache()
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    UDAFWithSql
  }
}

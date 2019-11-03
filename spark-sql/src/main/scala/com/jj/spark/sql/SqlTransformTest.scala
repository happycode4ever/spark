package com.jj.spark.sql

import com.sun.prism.PixelFormat.DataType
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object SqlTransformTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sqltest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  //注意样例类必须放在转换的方法体外面???
  private case class People(name:String,age:Int)
  import spark.implicits._
  def dataFrameTest: Unit = {
    //可以通过read读取不同类型的文件**有坑，默认装载的是Long类型
    val dataFrame = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\employees.json")
    dataFrame.show()
    dataFrame.printSchema()
    //创建临时表名，该SparkSession内有效
    dataFrame.createOrReplaceTempView("emp")
    //DSL模式 可以通过dataFrame直接操作方法
    dataFrame.select("name").show()
    //SQL模式通过session创建sql语句执行操作,表名就是上面定义的临时表名
    spark.sql("select * from emp").show()

  }

  def rdd2df: Unit = {
    //原生方式读取rdd转换tuple形式
    val rdd = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\people.txt")
      .map(line => {
        val fields = line.split(",")
        (fields(0),fields(1).trim.toInt)
      })
    //手动转换DF需要指定列名，也就是schema**注意需要SparkSession的隐身转换
    val df = rdd.toDF("name","age")
    df.show()
    //弱类型相当于JDBC的ResultSet，编译时不检查类型，运行时才检查
    //列是从0索引开始
    df.map(x=>(x.getString(0),x.getInt(1))).show()
    df.map(_.getAs[String]("name")).show()
//    df.map(x=>(x.getString(0),x.getString(1))).show()

    //转换rdd直接rdd即可 dfToRDD转换的是Row对象
    val rdd2 = df.rdd
    spark.stop()
  }

  def rdd2dfByReflection: Unit ={
    //通过case class反射出schema
    val rdd = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\people.txt")
      .map(line => {
        val fields = line.split(",")
        People(fields(0),fields(1).trim.toInt)
      })
    val df = rdd.toDF()
    df.show()
    df.printSchema()
  }

  def rdd2dfByCustom: Unit ={
    //构建schema StructType[List[StructField]]
    val schema = StructType(StructField("name",DataTypes.StringType)::StructField("age",DataTypes.IntegerType)::Nil)
    //封装data需要RDD[Row]
    val rdd = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\people.txt")
      .map(line => {
        val fields = line.split(",")
        Row(fields(0),fields(1).trim.toInt)
      })
    //通过sparkSession的data+schema的方式构建df
    val df = spark.createDataFrame(rdd,schema)
    df.show()
    df.printSchema()
  }

  def rdd2ds: Unit ={
//    import spark.implicits._
    //需要case Class指定schema//注意样例类必须放在转换的方法体外面???
    //原生方式读取rdd转换tuple形式
    val rdd = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\people.txt")
      .map(line => {
      val fields = line.split(",")
      People(fields(0),fields(1).trim.toInt)
    })
    val ds = rdd.toDS
    ds.show()
    ds.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
    //样例类已经规定了类型，可以直接使用
    ds.map(x=>(x.name,x.age)).show()

    //dsToRDD转回了样例类的类型
    val rdd2 = ds.rdd

    spark.stop()

  }

  def dfTods: Unit ={
    //由于默认装载的是Long类型，定义的样例类是Int类型，转换会失败
    val df = spark.read.json("H:\\bigdata-dev\\ideaworkspace\\spark\\spark-sql\\src\\main\\resources\\people.json")
    val ageColumn = $"age"
    val transDf = df.withColumn("age",ageColumn.cast(DataTypes.IntegerType))
    //**注意转换的列名和类型必须一致
    val ds = transDf.as[People]
    transDf.show()
    ds.show()

    transDf.printSchema()
    ds.printSchema()
    val df2 = ds.toDF()
  }

  def main(args: Array[String]): Unit = {
//        dataFrameTest
    //    rdd2df
    //    rdd2ds
//    dfTods
//    rdd2dfByReflection
    rdd2dfByCustom
  }
}

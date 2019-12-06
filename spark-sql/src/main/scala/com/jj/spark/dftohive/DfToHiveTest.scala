package com.jj.spark.dftohive

import java.io.File
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.junit.Test

class DfToHiveTest {
  /**
    * 0.准备好建表sql执行
    * 1.数据转换为row，根据hive表构建schema，通过spark.spark.createDataFrame构建df
    * 2.df.write直接写入hdfs分区目录
    * 3.执行添加分区sql
    */
  @Test
  def dfWrite: Unit ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DfToHiveTest")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val hdfsPath = "hdfs://hadoop112:9000/user/hive/wifi/external/qq"
    //创建外部表qq 注意long类型不支持需要是bigint
    val qqSql = s"create external table if not exists qq(rksj bigint,message string,rank double) partitioned by (year int,month int,day int) stored as parquet location '$hdfsPath'"
    spark.sql(qqSql)
    //构建数据转成row
    val data = Array((Instant.now().toEpochMilli,"aaaa",10.5),(Instant.now().toEpochMilli,"bbb",25.7),(Instant.now().toEpochMilli,"ccc",15.7),(Instant.now().toEpochMilli,"ddd",39.7))
    val rowRdd = sc.makeRDD(data).map(attrs => {
      Row(attrs._1,attrs._2,attrs._3)
    })
    val schema = StructType(StructField("rksj",DataTypes.LongType)::StructField("message",DataTypes.StringType)::StructField("rank",DataTypes.DoubleType)::Nil)
    val df = spark.createDataFrame(rowRdd,schema)
    //先写入数据再添加分区
    val year = 2019
    val month = 12
    val day = 12
    val hdfsParPath = hdfsPath + s"/$year/$month/$day"
    //df通过重新分区合并小文件(合并前可以合并小文件，通过使用HDFS的FileSystem的API)
    df.repartition(1).write.mode(SaveMode.Append).parquet(hdfsParPath)
    //先导入数据再添加分区
    spark.sql(s"alter table qq add if not exists partition(year=$year,month=$month,day=$day) location '$hdfsParPath'")

    spark.close()
  }

  /**
    * 合并前先读取df
    */
  @Test
  def dfRead: Unit ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DfToHiveTest")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val df = spark.read.parquet("hdfs://hadoop112:9000/user/hive/wifi/external/qq/2019/12/12")
    df.show()

    spark.close()
  }

  /**
    * 合并小文件测试
    */
  @Test
  def hdfsTest: Unit ={
    val hdfspPath = "hdfs://hadoop112:9000/user/hive/wifi/external/qq/2019/12/12"
    val fs = HDFSUtil.getFileSystem
//    val files = FileUtil.listFiles(new File(hdfspPath))
    //listFile只能列出文件
    val files = fs.listFiles(new Path(hdfspPath),true)
    while(files.hasNext){
      val file = files.next()
      println(file.getPath.getName)
    }
    println("------------------------------------------------")
    //liststatus还可以列出目录
    val statuses = fs.listStatus(new Path(hdfspPath))
    statuses.foreach(status => {
      println(status.getPath.getName)
    })
    val paths = FileUtil.stat2Paths(statuses)
    paths.foreach(path => {
      //是否递归删除，对于文件的path无所谓
      fs.delete(path,true)
    })
  }
}

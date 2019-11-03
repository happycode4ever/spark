package com.jj.rdd.adv

import org.apache.spark.{SparkConf, SparkContext}

object JDBCRddTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("cache")
  private val sc = new SparkContext(sparkConf)
  def read: Unit ={
    val rdd = new org.apache.spark.rdd.JdbcRDD (
      sc,
      //获取连接
      () => {
        Class.forName ("com.mysql.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection ("jdbc:mysql://hadoop112:3306/company", "root", "123456")
      },
      //基本sql
      "select * from staff where id >= ? and id <= ?;",
      //设定上下界
      1,
      5,
      1,
      //获取数据集的转换
      r => (r.getInt(1), r.getString(2), r.getString(3)))

    println (rdd.count () )
    rdd.foreach (println (_) )
    sc.stop ()
  }

  def write: Unit ={
    val data = List(("aaa","a1"),("bbb","b1"),("bbb","b2"),("aaa","a2"),("ccc","c1"),("ccc","c2"))
    val rdd = sc.makeRDD(data)
    rdd.foreachPartition(insertData)
    sc.stop()
  }

  def insertData(iterator: Iterator[(String,String)]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hadoop112:3306/rdd", "root", "123456")
    val ps = conn.prepareStatement("insert into rddtable(name,info) values (?,?)")
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.addBatch()
    })
    ps.executeBatch()
  }
  def main(args: Array[String]): Unit = {
//    read
    write
  }

}

package com.jj.spark.exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("exercise")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val context = spark.sqlContext
  private val sc = spark.sparkContext
  import spark.implicits._
  /**
    * 加载三张表数据并做转换dataset
    */
  def loadData: Unit ={
    val data = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\exercise\\tbDate.txt")
      .map(line=>{
        val attr = line.split(",")
        tbDate(attr(0),attr(1).trim().toInt, attr(2).trim().toInt,attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)
      }).toDS()

    data.createOrReplaceTempView("date")
    context.cacheTable("date")

    val stock = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\exercise\\tbStock.txt")
      .map(line=>{
        val attr = line.split(",")
        tbStock(attr(0),attr(1),attr(2))
      }).toDS()

    stock.createOrReplaceTempView("stock")
    context.cacheTable("stock")

    val stockDetail = sc.textFile("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-sql\\src\\main\\resources\\exercise\\tbStockDetail.txt")
      .map(line=>{
        val attr = line.split(",")
        tbStockDetail(attr(0),attr(1).trim().toInt,attr(2),attr(3).trim().toInt,attr(4).trim().toDouble, attr(5).trim().toDouble)
      }).toDS()

    stockDetail.createOrReplaceTempView("stockDetail")
    context.cacheTable("stockDetail")
    data.collect()
    stock.collect()
    stockDetail.collect()

  }

  def clearData: Unit ={
    context.clearCache()
    spark.stop()
  }

  case class tbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int)
  case class tbStock(ordernumber:String,locationid:String,dateid:String)
  case class tbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double)

  def test1: Unit ={
//    spark.sql("select * from date limit 5 ").show()
//    spark.sql("select * from stock limit 5 ").show()
//    spark.sql("select * from stockDetail limit 5 ").show()
//    spark.sql("select distinct ordernumber,count(ordernumber) over(partition by ordernumber) count  from stockDetail").show()
//    spark.sql("select ordernumber,count(ordernumber) over(partition by ordernumber) count  from stockDetail").show()

    //9.3计算所有订单中每年的销售单数、销售总额
    //按时间维度的年份分组，组内统计去重的订单数，以及销售额汇总
    spark.sql("select c.theyear year,count(distinct a.ordernumber) count,sum(a.amount) sum from stockDetail a,stock b,date c where a.ordernumber=b.ordernumber and b.dateid=c.dateid group by c.theyear order by c.theyear").show()
  }

  //9.4计算所有订单每年最大金额订单的销售额
  def test2: Unit ={
    //弄错需求 ，这个实现是每年单个订单销售额最高的订单的，有可能单个销售额相同，所以要求和得出销售总额
    /*spark.sql("select d.year,d.ordernumber,d.amount, sum(d.amount) over(partition by d.ordernumber) sum,d.rank from " +
      "(select c.theyear year,a.ordernumber,a.amount,rank() over(partition by c.theyear order by a.amount desc) rank " +
      "from stockDetail a,stock b,date c where a.ordernumber=b.ordernumber and b.dateid=c.dateid) d where d.rank = 1 order by d.year").show()*/
    //需求要求的是每年销售总额最高的订单以及实际的日期
    //1.先求每个订单的销售总额
    //2.关联表求出每年中该销售总额对应的订单表排名
    //3.提取每年排名为第一的订单相关信息然后按照年份排序
    spark.sql("select e.year,e.dateid,e.ordernumber,e.sumAmount,e.rank from " +
      "(select b.theyear year,b.dateid,d.ordernumber,d.sumAmount,rank() over(partition by b.theyear order by d.sumAmount desc) rank " +
      "from date b,stock c,(select a.ordernumber,sum(a.amount) sumAmount from stockDetail a group by a.ordernumber) d " +
      "where b.dateid=c.dateid and c.ordernumber=d.ordernumber) e where e.rank=1 order by e.year").show()

  }
  def test22: Unit ={
    //不使用over就需要缓存每年销售总额最高的订单表
    //1.求出每个订单的销售总额
    //2.关联表新增每个订单的时间维度信息
    //3.缓存这张带时间维度的订单销售总额表
    //4.按照年份分组求出每年最大的销售总额
    //5.关联缓存表提取对应的订单信息
    spark.sql("select b.theyear year,b.dateid,d.ordernumber,d.sumAmount from " +
      "date b,stock c,(select a.ordernumber,sum(a.amount) sumAmount from stockDetail a group by a.ordernumber) d " +
      "where b.dateid=c.dateid and c.ordernumber=d.ordernumber").createOrReplaceTempView("maxbyyear")
    context.cacheTable("maxbyyear")
    spark.sql("select b.year,b.dateid,b.ordernumber,b.sumAmount " +
      "from (select year,max(sumAmount) max from maxbyyear group by year) a " +
      "join maxbyyear b " +
      "on a.year=b.year and a.max=b.sumAmount " +
      "order by b.year").show()
  }

  //计算所有订单中每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）
  def test3: Unit ={
    /*val start = System.currentTimeMillis()
    spark.sql("select e.year,e.itemid,e.sumAmount,e.rank from " +
      "(select d.year,d.itemid,d.sumAmount,rank() over(partition by d.year order by sumAmount desc) rank from " +
      "(select distinct a.theyear year,c.itemid,sum(c.amount) over(partition by a.theyear,c.itemid) sumAmount " +
      "from date a,stock b,stockDetail c where a.dateid=b.dateid and b.ordernumber=c.ordernumber) d) e where e.rank=1 order by e.year").show()
    println(System.currentTimeMillis()-start)*/
    val start = System.currentTimeMillis()
    spark.sql("select e.year,e.itemid,e.sumAmount,e.rank from " +
      "(select d.year,d.itemid,d.sumAmount,rank() over(partition by d.year order by sumAmount desc) rank from " +
      "(select distinct a.theyear year,c.itemid,sum(c.amount) over(partition by a.theyear,c.itemid) sumAmount " +
      "from date a,stock b,stockDetail c where a.dateid=b.dateid and b.ordernumber=c.ordernumber) d) e where e.rank=1 order by e.year").show()
    println(System.currentTimeMillis()-start)
  }

  def test33: Unit ={
    val start = System.currentTimeMillis()
    spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.maxofamount " +
      "FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM stock a " +
      "JOIN stockDetail b ON a.ordernumber = b.ordernumber " +
      "JOIN date c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e " +
      "JOIN (SELECT d.theyear, MAX(d.sumofamount) AS maxofamount " +
      "FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount " +
      "FROM stock a JOIN stockDetail b ON a.ordernumber = b.ordernumber " +
      "JOIN date c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear ) f " +
      "ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear").show
    println(System.currentTimeMillis()-start)
  }

  def testGroupSum: Unit ={
    val start = System.currentTimeMillis()
//    spark.sql("select a.theyear year,c.itemid,sum(c.amount) from date a,stock b,stockDetail c where a.dateid=b.dateid and b.ordernumber=c.ordernumber group by a.theyear,c.itemid").show()
//      spark.sql("select a.theyear year,c.itemid,sum(c.amount) from date a join stock b on a.dateid=b.dateid join stockDetail c on b.ordernumber=c.ordernumber group by a.theyear,c.itemid").show()
//      spark.sql("select c.theyear year,b.itemid,sum(b.amount) from stock a join stockDetail b on a.ordernumber=b.ordernumber join date c on a.dateid=c.dateid group by c.theyear,b.itemid").show()
      spark.sql("select a.theyear year,c.itemid,c.amount from date a join stock b on a.dateid=b.dateid join stockDetail c on b.ordernumber=c.ordernumber").show()
    println(System.currentTimeMillis()-start)
  }
  def testGroupSum2: Unit ={
    val start = System.currentTimeMillis()
//    spark.sql("select a.theyear year,c.itemid,sum(c.amount) from date a,stock b,stockDetail c where a.dateid=b.dateid and b.ordernumber=c.ordernumber group by a.theyear,c.itemid").show()
//    spark.sql("SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount FROM stock a JOIN stockDetail b ON a.ordernumber = b.ordernumber JOIN date c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid").show
    spark.sql("SELECT c.theyear, b.itemid, b.amount AS SumOfAmount FROM stock a JOIN stockDetail b ON a.ordernumber = b.ordernumber JOIN date c ON a.dateid = c.dateid").show
    println(System.currentTimeMillis()-start)
  }


  def testOverSum: Unit ={
    val start = System.currentTimeMillis()
    spark.sql("select distinct a.theyear year,c.itemid,c.amount,sum(c.amount) over(partition by a.theyear,c.itemid) from date a join stock b on a.dateid=b.dateid join stockDetail c on b.ordernumber=c.ordernumber").show()
    println(System.currentTimeMillis()-start)
  }

  def main(args: Array[String]): Unit = {
    loadData
//    test1
//    test2
//    test22
//    test3
//    test33
    testGroupSum2
    testGroupSum
//    testOverSum
    clearData
  }
}


package com.jj.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


case class Score(name: String, clazz: Int, score: Int)

/**
  * 总结over窗口函数
  * rank跳跃排序，有两个第二名时后边跟着的是第四名
  * dense_rank() 连续排序，有两个第二名时仍然跟着第三名
  * row_number() 直接显示行号
  * first_value(field)/last_value(field)提取开头或者结尾行对应字段的值
  * lag|lead(field,offset,defaultValue) 向上或者向下提取offset行对应field字段的值，如果该行不存在取defaultValue否则就是null，注意over()里必须加order by
  * count(field)/min(field)/max(field)/sum(field)/avg(field)/rank()/dense_rank()/row_number/first_value(field)/last_value(field)/lag|lead(field,offset,defaultValue)
  *
  * over(partition by field) alias
  * 这类就是对表按照指定字段分区然后采用聚合函数分析得出结果
  *
  * over(partition by field order by field) alias
  * 这类就是对表按照指定字段分区并且排序，但是排序到当前行作为整体数据，然后采用聚合函数分析得出结果
  * 例如sum(score) over(partition by class order by name) 就是先按班级分区然后按姓名排序，排序到当前行进行叠加，产生连续叠加的效果
  *
  * 注意！！sum(socre) over(partition by class) 效率特别低，因为每行都需要组内叠加一次，时间翻了好几倍，这时候应该采用group by sum
  */
object OverFunctionTest {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OverFunctionTest")
  private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sc = spark.sparkContext
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = sc.makeRDD(Array( Score("a", 1, 80),
      Score("b", 1, 78),
      Score("c", 1, 95),
      Score("d", 2, 74),
      Score("e", 2, 92),
      Score("i", 3, 55),
      Score("g", 3, 99),
      Score("h", 3, 45),
      Score("f", 3, 99),
      Score("j", 3, 78)),6).toDF("name","class","score")
    df.createOrReplaceTempView("score")
    //缓存源数据
    spark.sqlContext.cacheTable("score")
    df.show()
    //求每个班级分数最高的学生
    // 原始的join方式 效率低
    spark.sql("select b.* from (select class,max(score) max from score group by class) a, score b where a.class = b.class and a.max = b.score").show()
    //采用窗口函数的方式新增一个rank列
    spark.sql("select name,class,score,max(score) over() rank from score").show()
    spark.sql("select name,class,score,avg(score) over(partition by class order by class,name) rank from score").show()
    spark.sql("select name,class,score,count(score) over(partition by class order by score desc) rank from score").show()

    spark.sql("select name,class,score,sum(score) over(partition by class) rank from score").show()
    /*
    +----+-----+-----+----+
    |name|class|score|rank|
    +----+-----+-----+----+
    |   a|    1|   80| 253|
    |   b|    1|   78| 253|
    |   c|    1|   95| 253|
    |   i|    3|   55| 376|
    |   g|    3|   99| 376|
    |   h|    3|   45| 376|
    |   f|    3|   99| 376|
    |   j|    3|   78| 376|
    |   d|    2|   74| 166|
    |   e|    2|   92| 166|
    +----+-----+-----+----+
     */
    spark.sql("select name,class,score,sum(score) over(partition by class order by score desc) rank from score").show()
    /*
    +----+-----+-----+----+
    |name|class|score|rank|
    +----+-----+-----+----+
    |   c|    1|   95|  95|
    |   a|    1|   80| 175|
    |   b|    1|   78| 253|
    |   g|    3|   99| 198|
    |   f|    3|   99| 198|
    |   j|    3|   78| 276|
    |   i|    3|   55| 331|
    |   h|    3|   45| 376|
    |   e|    2|   92|  92|
    |   d|    2|   74| 166|
    +----+-----+-----+----+
     */
    //高级应用 求每个班的平均值，并且按照班级排序
//    spark.sql("select name,class,score,avg(score) over(partition by class) avg from score").show()
//    spark.sql("select name,class,score,avg, dense_rank() over(order by avg) rank from (select name,class,score,avg(score) over(partition by class) avg from score) a").show()

    spark.sql("select name,class,score,lag(score,1,0) over(order by name) last from score").show()
    spark.sql("select name,class,score,lead(score,1,0) over(order by name) next from score").show()

    spark.sql("select count(distinct) over(partition by class) from score")

    spark.sqlContext.clearCache()
    spark.stop()
  }
}

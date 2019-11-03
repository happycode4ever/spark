package spark.mllib.fpgrowth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

object FPGrowth extends App{

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  //创建SparkContext
  val conf = new SparkConf().setMaster("local[4]").setAppName("FPGrowth")
  val sc = new SparkContext(conf)

  //加载数据样本
  val path = "H:\\bigdata-dev\\ideaworkspace\\spark\\spark-MLlib\\src\\main\\resources\\fpgrowth.txt";
  //创建交易样本
  val transactions = sc.textFile(path).map(_.split(" ")).cache()

  println(s"交易样本的数量为： ${transactions.count()}")

  //最小支持度(总数据行数*最小支持度=该项集最小支持次数)
  val minSupport = 0.3

  //计算的并行度
  val numPartition = 2

  //训练模型
  val model = new FPGrowth()
    .setMinSupport(minSupport)
    .setNumPartitions(numPartition)
    .run(transactions)

  //打印模型结果
  println(s"经常一起购买的物品集的数量为： ${model.freqItemsets.count()}")
  model.freqItemsets.collect().foreach { itemset =>
    println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
  }

  sc.stop()

  //交易样本的数量为： 6
  //经常一起购买的物品集的数量为： 18
  //[t], 3
  //[t,x], 3
  //[t,x,z], 3
  //[t,z], 3
  //[s], 3
  //[s,x], 3
  //[z], 5
  //[y], 3
  //[y,t], 3
  //[y,t,x], 3
  //[y,t,x,z], 3
  //[y,t,z], 3
  //[y,x], 3
  //[y,x,z], 3
  //[y,z], 3
  //[x], 4
  //[x,z], 3
  //[r], 3

}
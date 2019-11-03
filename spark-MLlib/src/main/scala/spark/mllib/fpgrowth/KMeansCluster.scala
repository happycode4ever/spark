package spark.mllib.fpgrowth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansCluster extends App{
  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  //创建SparkContext
  val conf = new SparkConf().setMaster("local[4]").setAppName("KMeans")
  val sc = new SparkContext(conf)

  //加载数据
  val path = "H:\\bigdata-dev\\ideaworkspace\\spark\\spark-MLlib\\src\\main\\resources\\kmeans.txt"
  val data = sc.textFile(path)
  //Vectors属于MLlib包 dense方法用于生成稠密向量
  val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

  //通过KMeans将数据分成两个阵营
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(parsedData, numClusters, numIterations)

  //输出当前的聚类中心
  clusters.clusterCenters.foreach(println _)

  val preditVector = Vectors.dense(Array(8.9, 7.2, 9.0))
  val index = clusters.predict(preditVector)
  println(s"Vector[8.9, 7.2, 9.0] 属于聚类索引为：${index} 中心坐标为：${clusters.clusterCenters(index)} 的簇")

  //计算误差平方和
  val WSSSE = clusters.computeCost(parsedData)
  println("误差平方和 = " + WSSSE)

  // 保存模型
//  clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//  val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")

  sc.stop()
}
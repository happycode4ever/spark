package spark.mllib.fpgrowth

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegressionML {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("LinearRegressionML").getOrCreate()
    //ml方式的数据集都是df
    val df = spark.read.format("libsvm").load("H:\\bigdata-dev\\ideaworkspace\\tanzhou\\spark\\spark-MLlib\\src\\main\\resources\\linear.txt")
    //构建模型
    val lr = new LinearRegression().setMaxIter(10000)
    val model = lr.fit(df)
    //由模型获取结果
    val summary = model.summary
    //结果评估模型
    summary.predictions.show()
    //获取均方根误差
    println(s"RMSE:${summary.rootMeanSquaredError}")
    spark.close()
  }
}

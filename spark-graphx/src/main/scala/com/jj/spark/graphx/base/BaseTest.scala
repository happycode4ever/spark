package com.jj.spark.graphx.base

import com.jj.spark.graphx.demo.DemoGraphxTest
import org.apache.spark.{SparkConf, SparkContext}

object BaseTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BaseTest")
  private val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    val graph = DemoGraphxTest.createDemoGraph(sc)

    DemoGraphxTest.printGraphTriplets(graph)

    //顶点数以及边个数
    val vertices = graph.numVertices
    val edges = graph.numEdges
    println(s"vertices=$vertices,edges=$edges")
    //每个顶点入度个数（边的箭头指向本顶点的个数）
    val inDegrees = graph.inDegrees
    println(s"inDegrees:${inDegrees.collect().mkString(",")}")
    //每个顶点出度个数（边的箭头指出本顶点的个数）
    val outDegrees = graph.outDegrees
    println(s"outDegrees:${outDegrees.collect().mkString(",")}")
    //每个顶点出入度个数
    val degrees = graph.degrees
    println(s"degrees:${degrees.collect().mkString(",")}")

    sc.stop()
  }

}

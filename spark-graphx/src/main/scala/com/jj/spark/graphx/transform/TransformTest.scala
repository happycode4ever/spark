package com.jj.spark.graphx.transform

import com.jj.spark.graphx.demo.DemoGraphxTest
import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}

object TransformTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformTest")
  private val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    val graph = DemoGraphxTest.createDemoGraph(sc)
    DemoGraphxTest.printGraphTriplets(graph)
    println("------------------------")
    //对每个顶点执行map操作 可以改变顶点属性但不能改id,可以使用匹配模式
    //    def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED]
    //    val graph1 = graph.mapVertices((id,vd)=>vd._1+":"+vd._2)
    val graph1 = graph.mapVertices({case (vertexId,(attr1,attr2)) => attr1+":"+attr2})
    DemoGraphxTest.printGraphTriplets(graph1)
    println("------------------------")
    //对每个边执行map操作 只可以改变边属性,不能用匹配模式，不知道为啥
    //    def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2]
    val graph2 = graph.mapEdges(edge=>edge.attr.toUpperCase)
//    graph.mapEdges({case Edge(_,_,attr) => attr}) 会报类型缺失
    DemoGraphxTest.printGraphTriplets(graph2)
    println("------------------------")
    //对每个三元组执行map操作只能修改边的属性
    //    def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]
    val graph3 = graph.mapTriplets(triplet=>triplet.attr+"abc")
    DemoGraphxTest.printGraphTriplets(graph3)
    println("------------------------")


    sc.stop()
  }

}

package com.jj.spark.graphx.struct

import com.jj.spark.graphx.demo.DemoGraphxTest
import org.apache.spark.graphx.VertexId
import org.apache.spark.{SparkConf, SparkContext}

object StructTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformTest")
  private val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    val graph = DemoGraphxTest.createDemoGraph(sc)
    DemoGraphxTest.printGraphAndVertices(graph)

    //1.反转图，每个边的方向反转
    val graph1 = graph.reverse
    DemoGraphxTest.printGraphAndVertices(graph1)

    //2.获取子图
    //  def subgraph(
    //      epred: EdgeTriplet[VD, ED] => Boolean = (x => true),//对边的筛选
    //      vpred: (VertexId, VD) => Boolean = ((v, d) => true))//对顶点的筛选 可以通过指定参数名跳过边或者顶点的筛选 例如subgraph(vpred = (v, d) => true)
    //    : Graph[VD, ED]

    //筛选奇数id的顶点，由于顶点都没了对应的边也会剔除
    val graph21 = graph.subgraph(vpred = (v,d) => v%2 != 0)
    DemoGraphxTest.printGraphAndVertices(graph21)
    //删选边不影响顶点的存在
    val graph22 = graph.subgraph(ed => ed.attr=="pi")
    DemoGraphxTest.printGraphAndVertices(graph22)

    //3.获取交集 this图与other图的交集，获取相同方向的边以及相同的顶点,边和顶点的属性保留自身
    // def mask[VD2: ClassTag, ED2: ClassTag](other: Graph[VD2, ED2]): Graph[VD, ED]

    //构建一个筛选了一个边的属性为pi变更为PI的子图,而且进一步筛选顶点作为判断依据
    val subgraph = graph.mapVertices({case (_,(attr1,attr2))=>s"$attr1:$attr2"}).mapEdges(edge=>{
      if(edge.attr=="pi")"PI"
      else edge.attr
    }).subgraph(epred = triplet => triplet.attr == "PI",vpred = (v,d) => v==5||v==7||v==2)
//      .reverse
    DemoGraphxTest.printGraphAndVertices(subgraph,"subgraph")

    DemoGraphxTest.printGraphAndVertices(subgraph.mask(graph),"mask1")
    DemoGraphxTest.printGraphAndVertices(graph.mask(subgraph),"mask2")

    //4.合并边 不能变更边的属性类型而且同一个分区内的相同顶点和方向的边才可以合并
//    def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]
    val groupgraph = graph.groupEdges((ed1,ed2)=>ed1+ed2)
    DemoGraphxTest.printGraphTriplets(groupgraph)

    sc.stop()
  }
}

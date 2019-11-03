package com.jj.spark.graphx.pregel

import com.jj.spark.graphx.demo.DemoGraphxTest
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PregelTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("PregelTest")
  private val sc = new SparkContext(sparkConf)

  def createGraph(sc:SparkContext)={
    //初始化顶点集合
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //创建顶点的RDD表示
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)

    //初始化边的集合
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(2L, 5L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //创建边的RDD表示
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //创建一个图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    val sourceId: VertexId = 5L // 定义源点
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    initialGraph
  }

  //求源点到各顶点的最短距离
  def grepelTest: Unit ={
    val graph = createGraph(sc)

    //def pregel[A: ClassTag](
    //      initialMsg: A,
    //      maxIterations: Int = Int.MaxValue,
    //      activeDirection: EdgeDirection = EdgeDirection.Either)(
    //      vprog: (VertexId, VD, A) => VD,
    //      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    //      mergeMsg: (A, A) => A)
    //    : Graph[VD, ED]
    //初始消息发无穷大，迭代次数无穷大，方向是往出度发
    val resGraph = graph.pregel(Double.MaxValue,Int.MaxValue,EdgeDirection.Out)(
      //mergeMsg聚合后的数据与本顶点属性合并，取路径数最小值
      (id,vd,a)=>Math.min(vd,a),
      //发送消息
      triplet=>{
        //当前顶点记录的路径数与边长度求和，小于目标顶点的路径数才发消息，证明这条路径更短
        //同时设置初始源顶点属性0，其他属性为正无穷的原因就是只有这个5顶点才发消息开始
        if(triplet.srcAttr+triplet.attr < triplet.dstAttr) {
          //发到目标顶点，距离是求和的距离
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        }else{
          //如果路径更长则不发消息
          Iterator.empty
        }
      },
      //多条消息到本顶点也取最小值
      (a1,a2)=>Math.min(a1,a2)
    ).mapVertices((_,vd)=>vd.toInt)
    DemoGraphxTest.printGraphAndVertices(resGraph)
  }

  def pregelTest2: Unit ={
    val graph = DemoGraphxTest.createPersonGraph(sc)
    //求追求者的最小年龄
    DemoGraphxTest.printGraphAndVertices(graph)
    val resGraph = graph.pregel[(String,Int)](("",Int.MaxValue),1,EdgeDirection.Out)(
      (id,ed,a)=>if(ed._2 < a._2) ed else a,
      triplet=>Iterator((triplet.dstId,triplet.srcAttr)),
      (a1,a2)=>if(a1._2<a2._2) a1 else a2
    )
    DemoGraphxTest.printGraphAndVertices(resGraph)
  }

  def main(args: Array[String]): Unit = {
    grepelTest
//   pregelTest2
  }
}

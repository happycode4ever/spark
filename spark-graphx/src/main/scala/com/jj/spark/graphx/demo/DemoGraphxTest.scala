package com.jj.spark.graphx.demo

import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DemoGraphxTest {

  def printGraphAndVertices[VD,ED](graph: Graph[VD,ED],graphName:String = null): Unit ={
    if(graphName!=null){
      println(graphName + ":")
    }
    println("triples:")
    graph.triplets.collect().foreach(triplet => {
      println(s"[src ${triplet.srcId}:${triplet.srcAttr}] edge[${triplet.attr}] dest[ ${triplet.dstId}:${triplet.dstAttr}] ")
    })
    println("vertices:")
    println(graph.vertices.collect().mkString(","))

    println("-----------------------------------")
  }

  def printGraphTriplets[VD,ED](graph:Graph[VD,ED],graphName:String = null): Unit ={
    if(graphName!=null){
      println(graphName + ":")
    }
    println("triples:")
    graph.triplets.collect().foreach(triplet => {
      println(s"[src ${triplet.srcId}:${triplet.srcAttr}] edge[${triplet.attr}] dest[ ${triplet.dstId}:${triplet.dstAttr}] ")
    })
    println("-----------------------------------")
  }

  def createDemoGraph(sc:SparkContext)= {
    //顶点 基本类型RDD[(VertexId, VD)] VertexId就是个声明类型 实际是Long表示顶点的ID,VD是顶点的属性，任意类型 优化类型 VertexRDD[VD]
    //    abstract class VertexRDD[VD](sc: SparkContext,deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps)
    val vertex = sc.makeRDD(Array(
      (3L,("rxin","student")),
      (7L,("jgonzal","postdoc")),
      (5l,("franklin","professor")),
      (2l,("istoica","professor"))
    ))
    //边 基本类型RDD[Edge[ED]] 优化类型EdgeRDD[ED]
    //    abstract class EdgeRDD[ED](sc: SparkContext,deps: Seq[Dependency[_]]) extends RDD[Edge[ED]](sc, deps)
    //边的数据类型是Edge样例类 包含原顶点和目标顶点，也就是方向是原顶点->目标顶点，以及边的属性ED
    //    case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED]
    //    (var srcId: VertexId = 0,var dstId: VertexId = 0,var attr: ED = null.asInstanceOf[ED]) extends Serializable
    val edge = sc.makeRDD(Array(
//      Edge(2l,5l,"colleague222222"),
      Edge(3l,7l,"collaborator"),
      Edge(5l,3l,"adviser"),
//      Edge(2l,5l,"colleague222222"),//目前测试发现就近的才会分区到一块
      Edge(2l,5l,"colleague"),
      //就近的才会合并
      Edge(5l,7l,"pi")
    ))
    //图 Graph是抽象类 可以由apply创建需要顶点和边信息
    //    abstract class Graph[VD: ClassTag, ED: ClassTag]
    //    def apply[VD: ClassTag, ED: ClassTag](vertices: RDD[(VertexId, VD)],edges: RDD[Edge[ED]],
    val graph = Graph(vertex,edge)

    //三元组 包含原顶点+属性 边属性 目标顶点+属性
    val triplets = graph.triplets

    graph
  }

  /**
    * 模拟婚恋网人员图
    * @param sc
    * @return
    */
  def createPersonGraph(sc:SparkContext) = {
    //初始化顶点集合 顶点属性包含姓名和年龄
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
      Edge(5L, 6L, 3),
      Edge(6L, 5L, 3)
    )

    //创建边的RDD表示
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //创建一个图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    graph
  }

  def createFromEdgeGraph(sc:SparkContext): Unit ={
    val edge = sc.makeRDD(Array(
      Edge(3l,7l,"collaborator"),
      Edge(5l,3l,"adviser"),
      Edge(2l,5l,"colleague"),
      Edge(5l,7l,"pi")
    ))
    // 根据边信息和默认顶点属性构建图，所有顶点属性均为同一属性
    //  def fromEdges[VD: ClassTag, ED: ClassTag](
    //      edges: RDD[Edge[ED]],
    //      defaultValue: VD,
    val graph = Graph.fromEdges(edge,"abc")
    printGraphAndVertices(graph,"fromEdgeGraph")
  }

  def createFromEdgeTupletGraph(sc:SparkContext): Unit ={
    //根据裸边创建图 只有源顶点和目标顶点，设定默认的顶点属性，然后边默认属性是1
    //  def fromEdgeTuples[VD: ClassTag](
    //      rawEdges: RDD[(VertexId, VertexId)],
    //      defaultValue: VD,
    val edge = sc.makeRDD(Array(
      //      Edge(2l,5l,"colleague222222"),
      Edge(3l,7l,"collaborator"),
      Edge(5l,3l,"adviser"),
      Edge(2l,5l,"colleague"),
      Edge(5l,7l,"pi")
    ))
    val rawEdge = edge.map({case Edge(srcId,dstId,attr) => (srcId,dstId)})
    val graph = Graph.fromEdgeTuples(rawEdge,"abc")
    printGraphAndVertices(graph,"fromEdgeTupletGraph")

  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("CreateGraphxTest")
    val sc = new SparkContext(sparkConf)

    val graph = createDemoGraph(sc)
    printGraphTriplets(graph)

    createFromEdgeGraph(sc)
    createFromEdgeTupletGraph(sc)

    sc.stop()
  }
}

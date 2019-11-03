package com.jj.spark.graphx.collect

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, TripletFields}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CollectTest {
  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("CollectTest")
  val sc = new SparkContext(sparkConf)

  def createGraph(sc:SparkContext) = {
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

  def collectTest1: Unit = {
    val graph = createGraph(sc)
    //获取每个顶点指定方向的邻居顶点信息，包含id和属性，以Array作为该顶点属性返回
    //方向包含In Out Either 注意both不能使用 直接会抛异常
    //def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]]
    val graph1 = graph.collectNeighbors(EdgeDirection.Either)
    val graph2 = graph.collectNeighbors(EdgeDirection.Out)
    graph1.collect().foreach({ case (id, array) =>
      val neighbourInfo = array.map({ case (id, attr) => s"[id:$id,attr:(${attr._1},${attr._2})]" }).mkString(",")
      println(s"$id => $neighbourInfo")
    })
    graph2.collect().foreach({ case (id, array) =>
      val neighbourInfo = array.map({ case (id, attr) => s"[id:$id,attr:(${attr._1},${attr._2})]" }).mkString(",")
      println(s"$id => $neighbourInfo")
    })
  }

  //根据方向求邻居顶点只提供id信息
  def collectTest2: Unit = {
    val graph = createGraph(sc)
    val graph1 = graph.collectNeighborIds(EdgeDirection.Either)
    graph1.collect().foreach({ case (id, array) => println(s"$id:[${array.mkString(",")}]") })
  }

  //根据方向求顶点相邻边的信息
  def collectTest3: Unit = {
    val graph = createGraph(sc)
    //def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]]
    val graph1 = graph.collectEdges(EdgeDirection.In)
    graph1.collect().foreach({ case (id, array) => println(s"$id:${array.mkString(",")}") })
  }

  //顶点间的通信
  //def aggregateMessages[A: ClassTag](
  //      sendMsg: EdgeContext[VD, ED, A] => Unit,
  //      mergeMsg: (A, A) => A,
  //      tripletFields: TripletFields = TripletFields.All)
  //    : VertexRDD[A]
  def aggregateTest: Unit = {
    val graph = createGraph(sc)
    //案例求追求者的最大年龄
    //每条边会应用sendMsg这个函数，该案例需要将源顶点的属性发送过来，也就是追求者的信息，组装成Array[(String,Int)]
    //org.apache.spark.graphx.EdgeContext.sendToDst/sendToSrc 表示边发送的方向 Src->Dst/Dst->Src
    //每个顶点收到的消息再应用mergeMsg这个函数，该案例需要将接受到的msg合并，所以就是合并所有Array[(String,Int)]的信息
    //优化属性tripletFields可以减少传输的数据量

    //注意需要指定A的类型也就是最后需要的结果类型
    val resGraph = graph.aggregateMessages[Array[(String, Int)]](
      context => context.sendToDst(Array(context.srcAttr))
      , (array1, array2) => array1 ++ array2//++是不可变数组和List的函数用于新生成一个
      , TripletFields.All//由于发送的消息只涉及源顶点，可以优化
    )
    resGraph.collect().foreach({ case (id, array) => println(s"$id:[${array.mkString(",")}]") })
    println("-------------------------------")
    val resRDD = resGraph.map({ case (id, array) => (id, array.sortWith((attr1, attr2) => attr1._2 > attr2._2).head) })
    println(resRDD.collect().mkString(","))
  }

  def main(args: Array[String]): Unit = {

    //    collectTest1
    //    collectTest2
    //    collectTest3
    aggregateTest
    sc.stop()
  }
}

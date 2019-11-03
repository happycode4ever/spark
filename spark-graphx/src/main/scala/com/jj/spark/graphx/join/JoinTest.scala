package com.jj.spark.graphx.join

import com.jj.spark.graphx.demo.DemoGraphxTest
import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("JoinTest")
  private val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    val graph = DemoGraphxTest.createDemoGraph(sc)

    val joinRDD = sc.makeRDD(Array((3L,13123)))

    DemoGraphxTest.printGraphAndVertices(graph)
    //join通过顶点id进行关联，可以拓展原VD的内容，不能改变类型，如果真需要类型有新增可以使用case class
    //def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD): Graph[VD, ED]
    val joinGraph = graph.joinVertices(joinRDD)((id,vd,u)=>(vd._1,vd._2+u))
    DemoGraphxTest.printGraphAndVertices(joinGraph)

    //外连接可以改变原有VD类型进行拓展，注意Option[U]，同顶点id连接的才会是Some(U)不然就是None，根据业务需要切换
    //例子就是变更原VD类型，并在没匹配上的顶点拓展了默认值
    //  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
    //      (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
    //    : Graph[VD2, ED]
    val outerGraph = graph.outerJoinVertices(joinRDD)({case (id,(attr1,attr2),u) => (attr1,attr2,u.getOrElse("default"))})
    DemoGraphxTest.printGraphAndVertices(outerGraph)
    sc.stop()
  }

}

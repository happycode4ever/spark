package com.jj.rdd.exercise

object ListSortTest {
  def main(args: Array[String]): Unit = {
    val list = List((1,2),(1,3),(2,1),(2,2))
    //sortwith方式排序
    val list2 = list.sortWith((x1,x2)=>x1._2>=x2._2)
    //隐式声明的方式排序
    implicit val myorder = new Ordering[(Int,Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
    }
    val list3 = list.sorted
    //根据函数的返回值排序
    val list4 = list.sortBy(x => x._2)
    println(list2)
    println(list3)
    println(list4)

  }
}

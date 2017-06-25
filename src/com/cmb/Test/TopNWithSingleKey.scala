package com.cmb.Test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * TopN列表，key唯一
  * Created by ChenNing on 2017/5/23.
  */
object TopNWithSingleKey {

  def main(args: Array[String]){

    //第一步；创建spark的配置对象sparkconf

    val conf = new SparkConf() //创建sparkconf对象
    conf.setAppName("TopNWithSingleKey").setMaster("local") //设置应用程序的名称

    //创建sparkcontext对象，sparkcontext是程序的唯一入口
    val sc = new SparkContext(conf)
    //由于是唯一键实际上与排序有关的只是value部分，简单化，输入数据为一列数字好了
    val lines = sc.textFile("E:\\JavaWork\\topNwithSingleKey.txt", 3)
    val N = 10   //闭包传值
    val rdd = lines.map(x => (x.toInt, N))
      .sortByKey(false).map(_._1).take(N)
      .foreach {
        println
      }


    val a = lines.map(x => x.toInt)

    //take用于获取RDD中从0到num-1下标的元素，不排序。
    a.take(2).foreach(println)
    //top函数用于从RDD中，按照默认（降序）或者指定的排序规则，返回前num个元素。
    a.top(2).foreach(println)
    //takeOrdered和top类似，只不过以和top相反的顺序返回元素。
    //Returns the first k (smallest) elements from this RDD as defined by the specified implicit Ordering[T] and maintains the ordering. This does the opposite of top.
    a.takeOrdered(2).foreach(println)

    /**
      * 在定义方法时，可以把最后一个参数列表标记为implicit，表示该组参数是隐式参数。
      * 一个方法只会有一个隐式参数列表，置于方法的最后一个参数列表。
      * 当调用包含隐式参数的方法是，如果当前上下文中有合适的隐式值，则编译器会自动为改组参数填充合适的值。
      * 如果没有编译器会抛出异常。
      */
    implicit val newSort = new Ordering[Int] {
      override def compare(a: Int, b: Int) =
        a.toString.compare(b.toString)
    } //隐式参数
    a.top(2).foreach(println)
    a.takeOrdered(2).foreach(println)
    sc.stop();
  }
}

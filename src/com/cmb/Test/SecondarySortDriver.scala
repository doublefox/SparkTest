package com.cmb.Test

import java.util.logging.Logger

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序
  * Created by ChenNing on 2017/5/13.
  */
object SecondarySortDriver {

  def main(args: Array[String]) {
    //第一步；创建spark的配置对象sparkconf

    val conf = new SparkConf() //创建sparkconf对象
    conf.setAppName("SecondarySortApp").setMaster("local") //设置应用程序的名称

    //创建sparkcontext对象，sparkcontext是程序的唯一入口

    val sc = new SparkContext(conf)

    val lines = sc.textFile("E:\\JavaWork\\aa.txt")

    println("groupByKey SecondarySort")
    //根据第三列升序，第一列降序 第二列升序
    val group = lines.map(line => {
      val lineArr = line.split(" ")
      (lineArr(2), (lineArr(0), lineArr(1)))
    }).groupByKey().sortByKey().mapValues((value) => {
      value.toList.sortWith((no1, no2) => {
        if (no1._1 != no2._1) {
          no1._1 > no2._1
        }
        else {
          no1._2 < no2._2
        }
      })
    }).foreachPartition(p => {
      println("#########")
      p.foreach(u => {
        println("$$$$$   " + u._1 + ":")
        //u._2.foreach(println)
        println(u)
      })
    })

    println("---------------------------")

    /**
      * sortBy: sortBy[B](f: (A) ⇒ B)(implicit ord: math.Ordering[B]): List[A] 按照应用函数f之后产生的元素进行排序
      * sorted： sorted[B >: A](implicit ord: math.Ordering[B]): List[A] 按照元素自身进行排序
      * sortWith： sortWith(lt: (A, A) ⇒ Boolean): List[A] 使用自定义的比较函数进行排序，比较函数boolean
      */
    /**
      * groupByKey是先移动key到一个节点上，当移动的数据量大于单台执行机器内存总量时Spark会把数据保存到磁盘上。
      * 不过在保存时每次会处理一个key的数据，所以当单个 key 的键值对超过内存容量会存在内存溢出的异常。
      * reduceByKey现在每个节点上执行计算，最后在汇总结果，所以reduceByKey更适合大数据集计算
      */
    println("---------------------------")
    println("WithSortkey SecondarySort")
    val pairWithSortkey = lines.map(line => (
      new SecondarySortKey(line.split(" ")(2), line.split(" ")(0).toInt, line.split(" ")(1).toInt), line
    ))


    val sorted = pairWithSortkey.sortByKey(false)

    val sortedResult = sorted.map(sortedline => sortedline._1.name + ":" + sortedline._2)
    sortedResult.collect.foreach(println)

    sc.stop
  }

}

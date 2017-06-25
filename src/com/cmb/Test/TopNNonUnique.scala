package com.cmb.Test


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.SortedMap
/**
  * Created by ChenNing on 2017/5/21.
  */
object TopNNonUnique {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TopNNonUnique").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(2)

    val input = sc.textFile("E:\\JavaWork\\topNwithUniqueKey.txt", 3)
    val kv = input.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toInt)
    })

    val uniqueKeys = kv.reduceByKey(_ + _)
    //import Ordering.Implicits._
    val partitions = uniqueKeys.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach { tuple =>
      {
        sortedMap += tuple.swap
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, String].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(N.value)


    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t ${v.mkString(" ")}")
    }


    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => (a + b)
//    combineByKey将类型为RDD[(K,V)]的数据处理为RDD[(K,C)]。这里的V和C可以是相同类型，也可以是不同类型。
//    这种数据处理操作并非单纯的对Pair的value进行map，而是针对不同的key值对原有的value进行联合（Combine）。因而，不仅类型可能不同，元素个数也可能不同
//    createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
//    mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
//    mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
    val moreConciseApproach = kv.combineByKey(createCombiner, mergeValue, mergeValue)
      .map(_.swap)
      .groupByKey()
      .sortByKey(false)
      .take(N.value)


    moreConciseApproach.foreach {
      case (k, v) => println(s"$k \t ${v.mkString(" ")}")
    }

    // done
    sc.stop()
  }
}

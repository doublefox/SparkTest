package com.cmb.Test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.SortedMap

/**
  * Created by ChenNing on 2017/5/21.
  */
object TopNUsingMapPartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TopNWithoutTake").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(10) //广播变量传值

    val input = sc.textFile("E:\\JavaWork\\topNwithSingleKey.txt", 3)
    val pair = input.map(x => (x.toInt, x))

    //import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach { tuple => {
        sortedMap += tuple
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
    //As with ++, returns a new collection containing the elements from the left operand followed by the elements from the right operand.
    //It differs from ++ in that the right operand determines the type of the resulting collection rather than the left one.
    //++ Returns a new traversable collection containing the elements from the left hand operand followed by the elements from the right hand operand.
    // The element type of the traversable collection is the most specific superclass encompassing the element types of the two operands.
    val finaltop10 = SortedMap.empty[Int, String].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(N.value)

    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k ")
    }

    sc.stop()
  }
}

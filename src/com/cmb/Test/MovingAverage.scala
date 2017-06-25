package com.cmb.Test

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
/**
  * Created by ChenNing on 2017/5/25.
  */
object MovingAverage {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MovingAverage").setMaster("local")
    val sc = new SparkContext(conf)

    val brodcastWindow = sc.broadcast(3)

    sc.textFile("E:\\JavaWork\\MovingAverage.txt")
      .map(line => {
        val tokens = line.split(",")
        val timestamp = DateTime.parse(tokens(1), DateTimeFormat.forPattern("yyyy-MM-dd")).getMillis
        (CompositeKey(tokens(0), timestamp), TimeSeriesData(timestamp, tokens(2).toDouble))
      })
      // 按照股票代码和时间进行排序
      // 如果需要在repartition重分区之后，还要进行排序，建议直接使用repartitionAndSortWithinPartitions算子。
      // 因为该算子可以一边进行重分区的shuffle操作，一边进行排序
      .repartitionAndSortWithinPartitions(new CompositeKeyPartitioner(5))
      .map(k => (k._1.stockSymbol, k._2))
      .groupByKey()
      .mapValues(itr => {
        val queue = new mutable.Queue[Double]()
        /*
        针对每一次 for 循环的迭代, yield 会产生一个值，被循环记录下来 (内部实现上，像是一个缓冲区).
        当循环结束后, 会返回所有 yield 的值组成的集合.
        返回集合的类型与被遍历的集合类型是一致的.
         */
        for (tsd <- itr) yield {
          queue.enqueue(tsd.closingStockPrice)
          if (queue.size > brodcastWindow.value)
            queue.dequeue()

          (new DateTime(tsd.timeStamp).toString("yyyy-MM-dd"), tsd.closingStockPrice, queue.sum / queue.size)
        }
      })
      .flatMap(kv => kv._2.map(v => (kv._1, v._1, v._2, v._3)))
      .foreach(println)

    sc.stop()
  }

  // 自定义分区
  class CompositeKeyPartitioner(partitions: Int) extends Partitioner {
    //你想要创建分区的个数
    override def numPartitions: Int = partitions

    /*
    这个函数需要对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1
     */
    override def getPartition(key: Any): Int = key match {
      case CompositeKey(stockSymbol, _) => math.abs(stockSymbol.hashCode % numPartitions)
      case null => 0
      case _ => math.abs(key.hashCode() % numPartitions)
    }

    override def equals(other: Any): Boolean = other match {
      case h: CompositeKeyPartitioner => h.numPartitions == numPartitions
      case _ => false
    }

    override def hashCode: Int = numPartitions
  }

}

case class CompositeKey(stockSymbol: String, timeStamp: Long)

object CompositeKey {
  //隐式转换函数:在同一个作用域下面，一个给定输入类型并自动转换为指定返回类型的函数
  //这个函数和函数名字无关，和入参名字无关，只和入参类型以及返回类型有关。注意是同一个作用域。
  implicit def ord[A <: CompositeKey]: Ordering[A] = Ordering.by(fk => (fk.stockSymbol, fk.timeStamp))
}

/*
Case Class的特别之处在于：

编译器会为Case Class自动生成以下方法：
equals & hashCode
toString
copy
编译器会为Case Class自动生成伴生对象
编译器会为伴生对象自动生成以下方法
apply
unapply
这意味着你可以不必使用new关键字来实例化一个case class.
case class的类参数在不指定val/var修饰时，会自动编译为val，即对外只读，如果需要case class的字段外部可写，可以显式地指定var关键字！
简单地总结起来就是：让编译器帮忙自动生成常用方法！
 */
case class TimeSeriesData(timeStamp: Long, closingStockPrice: Double)

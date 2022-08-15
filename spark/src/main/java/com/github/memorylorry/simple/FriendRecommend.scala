package com.github.memorylorry.simple

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

import java.util

object FriendRecommend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Test")
    val spark = new SparkContext(config = conf)

    // tom hello hadoop cat
    val relation_ship = (x: String, y: String) => if (x.compareTo(y) < 0) String.format("%s_%s", x, y) else String.format("%s_%s", y, x)

    val rdd = spark.textFile("hdfs://n1:9000/data/friend")
    var rdd2 = rdd.flatMap(r => {
      val ans: java.util.List[(String, Int)] = new util.ArrayList[(String, Int)]()

      val vs = r.split(" ")

      for (i <- 1 until vs.size) {
        ans.add((relation_ship(vs(0), vs(i)), 0))

        for (j <- i + 1 until vs.size) {
          ans.add((relation_ship(vs(i), vs(j)), 1))
        }
      }

      ans.asScala.toList
    })

    rdd2 = rdd2.groupByKey().flatMap(r => {
      val rows: List[Int] = r._2.toList
      var flag = true
      var ans = 0

      for (i <- 0 until rows.size) {
        if (rows(i) == 0) {
          flag = false
        }
        ans += 1
      }

      if (flag) List((r._1, ans)) else List()
    })

    var rdd3 = rdd2.flatMap(r => {
      val ans: java.util.List[(String, (String, Int))] = new java.util.ArrayList()
      val vs = r._1.split("_")
      ans.add((vs(0), (vs(1), r._2)))
      ans.add((vs(1), (vs(0), r._2)))
      ans.asScala.toList
    })

    var rdd4 = rdd3.groupByKey().flatMap(r => {
      var rows: List[(String, Int)] = r._2.toList
      rows.sortWith((x: (String, Int), y: (String, Int)) => x._2 > y._2)
      rows = rows.take(2)

      var ans: java.util.List[(String, Int)] = new util.ArrayList()
      for (i <- 0 until rows.size) {
        ans.add((r._1 + "_" + rows(i)._1, rows(i)._2))
      }
      ans.asScala.toList
    })
    rdd4 = rdd4.sortByKey(ascending = true)

    rdd4.foreach(r => {
      println(r._1 + " " + r._2)
    })
  }
}

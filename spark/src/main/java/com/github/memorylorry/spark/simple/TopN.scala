package com.github.memorylorry.spark.simple

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

import java.util

object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopN").setMaster("local[4]")
    val sc = new SparkContext(config = conf)

    val rdd = sc.textFile(String.format("hdfs://n1:9000%s", args(0)))
    val rdd2 = rdd.map(r => {
      val vs = r.split(","); (vs(0), Integer.parseInt(vs(1)))
    }).groupByKey().flatMap(r => {
      val res: List[Int] = r._2.toList.sorted.reverse.take(2)
      val ans: java.util.List[(String, Int)] = new util.ArrayList[(String, Int)]()
      for (i <- 0 until res.size) {
        val row = res(i)
        ans.add((r._1, row))
      }
      ans.asScala
    }).sortByKey().collect()

    rdd2.foreach(r => {
      println(r._1 + " " + r._2)
    })
  }
}

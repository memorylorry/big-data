package com.github.memorylorry.spark.simple

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(path = String.format("hdfs://n1:9000%s", args(0)))
    val rdd2 = rdd.flatMap(r => r.split(" ")).map(r => (r, 1)).reduceByKey((x, y) => x + y).sortBy(r => r._2).collect()
    rdd2.foreach(r => {
      println(r._1 + " " + r._2)
    })
  }
}

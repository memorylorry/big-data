package com.github.memorylorry.spark.simple

import org.apache.spark.{SparkConf, SparkContext}

object SecondSortMR {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSortMR").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(String.format("hdfs://n1:9000%s", "/data/sorts.txt"))
    val rdd2 = rdd.map(line => {
      val vs = line.split(' ')
      (new SecondSort(vs(0).toInt, vs(1).toInt), line)
    }).sortByKey().map(r => r._2).collect()
    rdd2.foreach(r => println(r))
  }
}

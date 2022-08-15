package com.github.memorylorry.datalean

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 此处探究压缩数据造成的数据倾斜。
 * 输入的文件为/animal/compressed下的所有文件，这些文件的特征如下：
 * - part1-3和part5-7均约300MB
 * - part4.txt.gz为7.8MB，解压后5.3GB
 * 由于part4为7.8MB，小于最小切片size，因此part4会被分到了1个task上，这个task在处理该文件时，会先解压再读取，由于解压耗时较多，因此该task会花费较多的时间。从而造成数据倾斜。
 */

object s1 {
  def main(args: Array[String]): Unit = {
    val compressed = if(args.size>0) args(0).toInt else 0
    val root = if(compressed>0) "hdfs://n1:9000/animal/compressed" else "hdfs://n1:9000/animal/uncompressed"
    println(root)

    val conf = new SparkConf().setMaster("spark://n1:7077").setAppName("ReadDataLean")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(root).map(r=>(r, 1)).reduceByKey((x, y)=>x+y)
    rdd.foreach(r=>{
      println(r._1 + " " + r._2)
    })
  }
}

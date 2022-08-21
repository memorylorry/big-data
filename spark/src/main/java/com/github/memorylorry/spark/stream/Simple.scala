package com.github.memorylorry.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Simple {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(line=>line.split(' ')).map(w=>(w, 1)).reduceByKey((x,y)=>x+y)

    words.foreachRDD(r=>{
      r.foreach(w=>{
        println(w. _1+" "+w._2)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

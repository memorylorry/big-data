package com.github.memorylorry.datalean

import org.apache.spark.{SparkConf, SparkContext}


object s2 {
  def main(args: Array[String]): Unit = {
    val MAP_JOIN = if(args.size > 0) args(0).toInt else 0
    println("MAP_JOIN: "+MAP_JOIN)

    val conf = new SparkConf().setMaster("spark://n1:7077").setAppName("JoinDataLean")
    val sc = new SparkContext(conf)

    val user = sc.textFile("hdfs://n1:9000/store/user.txt")
    val order = sc.textFile("hdfs://n1:9000/store/order")

    val users = user.map(u=>{
      val vs = u.split(",")
      (vs(0).toInt, vs(1))
    })
    val orders = order.map(o=>{
      val vs = o.split(",")
      (vs(0).toInt, vs(1).toFloat)
    })

    if(MAP_JOIN > 0){
      val broadcastVal = sc.broadcast(users.collectAsMap())

      val user_order = orders.map(r=>(broadcastVal.value(r._1), r._2)).reduceByKey((x,y)=>x+y)
      user_order.foreach(r=>{
        println(r._1+" "+r._2)
      })
    }else{
      val user_order = users.join(orders).map(r=> (r._2._1, r._2._2)).reduceByKey((x,y)=>x+y)
      user_order.foreach(r=>{
        println(r._1+" "+r._2)
      })
    }
  }
}

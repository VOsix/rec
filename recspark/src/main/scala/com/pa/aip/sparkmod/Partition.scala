package com.pa.aip.sparkmod

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * partitionBy 重新分区， repartition默认采用HashPartition分区
  * 自己设计合理的分区方法(比如数量比较大的key加个随机数,随机分到更多的分区,处理数据倾斜更彻底一些)
  * com.pa.aip.sparkmod Created by bookforcode on 2019-06-09.
  */
object Partition {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("partitionTest")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd: RDD[(String, String)] = sc.makeRDD(Array(("1", "2"), ("2", "3"), ("2", "4")), 3)
    //查看rdd1中每个分区的元素
    val rdd1 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))



    rdd1.mapPartitionsWithIndex {
      (partIdx, iter) => {
        val part_map = scala.collection.mutable.Map[String, List[(String, String)]]()
        while (iter.hasNext) {
          val part_name = "part_" + partIdx
          var elem: (String, String) = iter.next()
          if (part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(String, String)] {
              elem
            }
          }
        }
        part_map.iterator
      }
    }.collect
      .foreach(println)


  }


}

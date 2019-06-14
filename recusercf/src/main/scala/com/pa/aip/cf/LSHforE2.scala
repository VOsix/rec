package com.pa.aip.cf

import org.apache.spark.ml.linalg.{Vector, Vectors}
import breeze.stats.distributions.{Gaussian, Uniform}
import java.text.NumberFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
  * com.pa.aip.cf Created by bookforcode on 2019/6/8.
  * 实现欧氏空间的LSH算法，添加了大量优化，详细信息可以参考下面的文献
  *
  * http://bourneli.github.io/probability/lsh/2016/09/15/lsh_eulidian_1.html
  * http://bourneli.github.io/probability/lsh/2016/09/22/lsh_eulidian_2.html
  * http://bourneli.github.io/probability/lsh/2016/09/23/lsh_eulidian_3.html
  * http://bourneli.github.io/probability/lsh/2016/10/01/lsh_eulidian_4.html
  *
  * @param distince     每个相似对的最大距离小雨等于distance
  * @param lshWidth     映射后的桶的宽度，文献中对应w
  * @param bucketWidth  桶内，相同lsh的个数，文献中对应k
  * @param bucketSize   桶的个数，文献中对应L
  * @param storageLevel 缓存策略，数据量小为Memory，数据量大为Memory_and_disk
  * @param parts        RDD分区数量，用于设置并发
  * @param lshUpBound   当个桶样本上限，一旦突破此上限，采取随机取样策略
  * @param lshRandom    随机选取的数量，随机的个数
  * @param mergeSize    LSH表格缓存个数
  * @param maxSize      每个样本，最多保留maxSize个相似对
  */
class LSHforE2(distince: Double, lshWidth: Double,
               bucketWidth: Int = 10, bucketSize: Int = 30,
               storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
               parts: Int = 200, lshUpBound: Int = 1000,
               lshRandom: Int = 100, mergeSize: Int = 8, maxSize: Int = 100
              ) extends Serializable {
  assert(distince > 0, s"distince = $distince must be greater than 0")
  assert(lshWidth > 0, s"$lshWidth must be greater than 0")
  assert(bucketWidth > 0, s"$bucketWidth must be greater than 0")
  assert(bucketSize > 0, s"$bucketSize must be greater than 0")
  assert(lshRandom < lshUpBound, s"lshRandom $lshRandom should be less than lshUpBound $lshUpBound")

  def lsh[K: ClassTag](raw: RDD[(K, Vector)]): RDD[(K, K, Double)] = {

    //分区加缓存，用于后面的join
    val data: RDD[(K, Vector)] = raw.partitionBy(new HashPartitioner(parts)).persist(storageLevel)
    val nf: NumberFormat = java.text.NumberFormat.getIntegerInstance

    //特征长度
    val vectorWidth = data.first._2.size
    assert(vectorWidth > 0, s"Vector width is $vectorWidth")

    //生成所有的Hash参数
    val normal = Gaussian(0, 1) //随机生成投影向量
    val uniform = Uniform(0, lshWidth) //生成随机偏差
    val hashSeq: IndexedSeq[(Vector, Double)] = for (i <- 0 until bucketWidth * bucketSize)
      yield (Vectors.dense(normal.sample(vectorWidth).toArray), uniform.sample)
    val hashFunctionBC = data.sparkContext.broadcast(hashSeq)

    //计算lsh值
    val lshRDD = data.map { case (id, vector) =>
      val hashFunctions = hashFunctionBC.value
      val hashValues = hashFunctions.map {
        case (project: Vector, offset: Double) =>
          val dotProduct = project.toArray.zip(vector.toArray).map(x => x._1 + x._2).sum
          ((dotProduct + offset) / lshWidth).floor.toLong
      }
      (id, hashValues.toArray.grouped(bucketWidth).toArray)
    }.persist(storageLevel)
    println("LSH Tag")
    lshRDD.take(10).map(x =>
      "%s -> %s".format(x._1, x._2.map(_.mkString(" ")).mkString(",")))
      .foreach(println)
    val dataSize = lshRDD.count
    val allPairSize = dataSize * (dataSize - 1) / 2d
    println("LSH Size:" + nf.format(dataSize))

    //计算所有的lsh桶
    val bucketBuffer = ArrayBuffer[RDD[(K, (K, Double))]]()
    (0 until bucketSize).foreach(i => {
      val begin = System.currentTimeMillis()

      //统计lsh后每个ID的聚集数据量
      val idGroupRDD: RDD[Array[K]] = lshRDD.map(x => (x._2(i).mkString(","), x._1))
        .groupByKey(parts)
        .map(_._2.toArray)
        .filter(_.length > 1)
        .persist(storageLevel)
      val numGroup = idGroupRDD.count()
      if (numGroup > 0) {
        val meanGroup = idGroupRDD.map(_.length).mean()
        val stdevGroup = idGroupRDD.map(_.length).stdev()
        val maxGroup = idGroupRDD.map(_.length).max
        val pairsCount = idGroupRDD.map(_.length).map(x => x * (x - 1)).sum()
        println(
          s"""
             |Mean Group Size: $meanGroup
             |Standard Deviation: $stdevGroup
             |Max Group Size: $maxGroup
             |Pairs Count: $pairsCount
           """.stripMargin
        )
      } else {
        s"""
           |Mean Group Size: 0
           |Standard Deviation:0
           |Max Group Size: 0
           |Pairs Count: 0
           """.stripMargin
      }

      //组合相似对
      val similarPairs: RDD[(K, K)] = idGroupRDD.flatMap(neighbors => {
        if (neighbors.length <= lshUpBound) {
          //候选集不多，排列组合任意两个id
          val pairs = neighbors.combinations(2)
            .map(x => (x(0), x(1))).toArray
          pairs ++ pairs.map(_.swap)
        } else {
          //候选集过多，随机选取，避免排列组合爆炸
          val rand = new Random(System.currentTimeMillis())
          val allRandomPairs = ArrayBuffer[(K, K)]()
          for (key <- neighbors) {
            val randomRhs = (for (_ <- 0 until lshRandom)
              yield neighbors(rand.nextInt(neighbors.length)))
              .distinct.map(x => (key, x))
            allRandomPairs.appendAll(randomRhs)
          }
          allRandomPairs.toArray[(K, K)]
        }
      }).partitionBy(new HashPartitioner(parts)).persist(storageLevel)

      //剔除过多数据
      val reduceSimilarPairs = similarPairs.join(data)
        .map({ case (srcKey, (dstKey, srcVec)) => dstKey -> (srcKey, srcVec) })
        .partitionBy(new HashPartitioner(parts))
        .join(data)
        .map({ case (dstKey, ((srcKey, srcVec), dstVec)) =>
          val distince = scala.math.sqrt((srcVec.toArray zip dstVec.toArray)
            .map(x => scala.math.pow(x._1 - x._2, 2)).sum)
          srcKey -> (dstKey, distince)
        })
        .groupByKey(parts)
        .flatMap({ case (srcKey, distList) => distList.toArray
          .filter(_._2 < distince)
          .sortBy(_._2)
          .slice(0, maxSize)
          .map({ case (dstKey, dist) => (srcKey, (dstKey, dist)) })
        }).partitionBy(new HashPartitioner(parts)).persist(storageLevel)

      val currentResultSize = reduceSimilarPairs.count()
      val end = System.currentTimeMillis()
      val timeCost = (end - begin) / 1000d
      println(s"===========Rount $i, Result Size = %s, time cost: $timeCost s"
        .format(nf.format(currentResultSize)))
      idGroupRDD.unpersist(false)
      bucketBuffer.append(reduceSimilarPairs)

      //合并中间表格，节省空间
      if ((bucketBuffer.length >= mergeSize || i == bucketSize - 1) && bucketBuffer.length > 1) {
        val mergeBegin = System.currentTimeMillis()

        val merged = data.sparkContext.union(bucketBuffer)
          .groupByKey(parts)
          .flatMap({ case (srcKey, dstList) => dstList.toArray
            .distinct
            .sortBy(_._2)
            .slice(0, maxSize)
            .map({ case (dstKey, dist) => (srcKey, (dstKey, dist)) })
          })
          .partitionBy(new HashPartitioner(parts)).persist(storageLevel)

        val mergedCount = merged.count()
        println(s"Merged Count: %s".format(nf.format(mergedCount)))

        bucketBuffer.foreach(_.unpersist(false))
        bucketBuffer.clear
        bucketBuffer.append(merged)

        val mergeEnd = System.currentTimeMillis()
        val timeCost = (mergeEnd - mergeBegin) / 1000d
        println(s"Merge Time Cost: $timeCost")
      }
    })

    assert(1 == bucketBuffer.size, "Bucket Size is great than 1")
    val finalResult = bucketBuffer(0)
      .map({ case (srcKey, (dstKey, dist)) => (srcKey, dstKey, dist) })
      .persist(storageLevel)
    bucketBuffer.clear()
    val distinctLshParisSize = finalResult.count()
    println(s"Distinct Lsh Pair Size %s, rate = %.3f".format(
      nf.format(distinctLshParisSize), distinctLshParisSize / allPairSize
    ))
    finalResult


  }


}

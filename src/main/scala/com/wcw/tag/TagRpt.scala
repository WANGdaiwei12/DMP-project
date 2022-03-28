package com.wcw.tag

import java.util.UUID

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 2:29
 * @version: V1.0
 * @modified By:
 */
object TagRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |com.dahua.dmp.tag.TagRpt
          |缺少参数
          |inputPath
          |app_mapping
          |stopword
          |outputPath
        """.stripMargin)
      // 写redis也行，我们要写入hbase。  写入hbase.商圈。GEOHash算法。百度有
      sys.exit()
    }

    // 封装到Array
    var Array(inputPath, app_mapping, stopword, outputPath) = args

    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 读perquet文件。
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext

    // 1： 收集 appName,stopword
    val appMap = sc.textFile(app_mapping).map(line => {
      val fields: Array[String] = line.split("[:]")
      (fields(0), fields(1))
    }).collect().toMap
    val stopwordMap: Map[String, Int] = sc.textFile(stopword).map((_, 0)).collect().toMap

    // 广播变量
    val broadcastAppMap = sc.broadcast(appMap)
    val broadcastStopwordMap: Broadcast[Map[String, Int]] = sc.broadcast(stopwordMap)

    val df: DataFrame = spark.read.parquet(inputPath)
    val tags: Dataset[(String, List[(String, Int)])] =
      df.where(TagUtil.tagUserIdFilterParam)
        .map(row => {
          // 广告请求：
          val adsMap: Map[String, Int] = AdsTags.makeTags(row)
          // app标签
          val appMap: Map[String, Int] = AppTags.makeTags(row, broadcastAppMap.value)
          // 驱动标签
          val driverMap: Map[String, Int] = DriverTag.makeTags(row)
          // 关键字请求
          val keyMap: Map[String, Int] = KeyTags.makeTags(row, broadcastStopwordMap.value)
          //PC标签
          val pcMap: Map[String, Int] = PCTags.makeTags(row)
          // 商圈标签
          val sqMap: Map[String, Int] = SqTag.makeTag(row)

          if (TagUtil.getUserId(row).size > 0) {
            (TagUtil.getUserId(row)(0), (appMap ++ driverMap ++ keyMap ++ pcMap ++sqMap).toList)
          } else {
            (UUID.randomUUID().toString.substring(0, 6),
              (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ++ sqMap).toList)
          }
        })

    tags.rdd.reduceByKey {
      case (a, b) => {

        (a ++ b).groupBy(_._1).map { x =>
          (x._1, x._2.map(_._2).sum)
        }
      }.toList
    }.saveAsTextFile(outputPath)


  }
}

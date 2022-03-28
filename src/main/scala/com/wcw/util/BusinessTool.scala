package com.wcw.util

import ch.hsr.geohash.GeoHash
import com.wcw.shangquan.SNTools
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

/**
 * 将商圈经纬度使用GEOHash算法,写入到Redis
 *
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 4:31
 * @version: V1.0
 * @modified By:
 */
object BusinessTool {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |com.dahua.tools
          |缺少参数
          |inputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputPath) = args

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //  读取parquet文件。
    spark.read.parquet(inputPath)
      // 获得lat  long
      .select("lat", "longitude")
      .where("lat > 3 and lat < 54 and longitude > 73 and longitude < 136")
      .distinct() // 写入redis.考虑。 开启连接，关闭连接。
      .foreachPartition(ite => {
        val jedis: Jedis = JedisUtil.getJedis
        ite.foreach(row => {
          val lat: String = row.getAs[String]("lat") //维度
          val longat: String = row.getAs[String]("longitude") //经度
          //          调用百度api获取商圈信息
          val business: String = SNTools.getBusiness(longat + "," + lat)
          if (StringUtils.isNotBlank(business)) {
            //            GeoHash计算hash码
            val geohash: GeoHash = GeoHash.withCharacterPrecision(lat.toDouble, longat.toDouble, 8)
            val code: String = geohash.toBase32
            jedis.hset("shangquan", code, business)
            println(business)
          }
        })
        jedis.close()
      })

    sc.stop()
    spark.stop()
  }
}

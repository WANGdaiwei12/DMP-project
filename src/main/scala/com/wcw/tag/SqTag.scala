package com.wcw.tag

import ch.hsr.geohash.GeoHash
import com.wcw.shangquan.SNTools
import com.wcw.util.JedisUtil
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 4:09
 * @version: V1.0
 * @modified By:
 */
object SqTag extends MakeTag {
  override def makeTag(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val longitude: String = row.getAs[String]("longitude")// 经度
    val lat: String = row.getAs[String]("lat")// 维度
//    不希望一直连接百度，就从redis中读取
//    val business: String = SNTools.getBusiness(longitude + "," + lat)
    if(lat.toDouble > 3 && lat.toDouble < 54 && longitude.toDouble > 73 && longitude.toDouble < 136){
      val geoHash: GeoHash = GeoHash.withCharacterPrecision(lat.toDouble, longitude.toDouble, 8)
      val code: String = geoHash.toBase32
      val jedis: Jedis = JedisUtil.getJedis
      val business: String = jedis.get(code)
      map += "SN"+business->1
      jedis.close()
    }else{
      println()
    }
    map

  }
}

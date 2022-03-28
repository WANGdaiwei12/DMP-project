package com.wcw.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 2:11
 * @version: V1.0
 * @modified By:
 */
object PCTags extends TagTrait {

  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val proName: String = row.getAs[String]("provincename")
    val cityName: String = row.getAs[String]("cityname")

    if(StringUtils.isNotEmpty(proName)) map += "ZP"+proName -> 1
    if(StringUtils.isNotEmpty(cityName)) map += "ZC"+cityName -> 1

    map
  }
}

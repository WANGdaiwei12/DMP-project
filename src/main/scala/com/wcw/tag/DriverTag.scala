package com.wcw.tag

import org.apache.spark.sql.Row

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 2:00
 * @version: V1.0
 * @modified By:
 */
object DriverTag extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val client: Int = row.getAs[Int]("client")
    val networkmannername: String = row.getAs[String]("networkmannername")
    val ispname: String = row.getAs[String]("ispname")

    client match{
      case 1 => map += " D00010001"->1
      case 2 => map += " D00010002"->1
      case 3 => map += " D00010003"->1
      case _ => map += " D00010004"->1
    }

    networkmannername match{
      case "WIFI" => map += "D00020001" -> 1
      case "4G" => map += "D00020002" -> 1
      case "3G" => map += "D00020003" -> 1
      case "2G" => map += "D00020004" -> 1
      case _  => map += "D00020005" -> 1
    }

    ispname match{
      case "移动" => map += "D00030001" -> 1
      case "联通" => map += "D00030002" -> 1
      case "电信" => map += "D00030003" -> 1
      case _  => map += "D00030004" -> 1

    }

    map



  }
}

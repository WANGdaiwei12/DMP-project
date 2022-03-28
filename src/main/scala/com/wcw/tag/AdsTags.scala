package com.wcw.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**1.2广告标签需求
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 1:31
 * @version: V1.0
 * @modified By:
 */
object AdsTags extends TagTrait{
  //标签格式： LC03->1 或者 LC16->1）xx 为数字，小于 10 补 0
  //1)告位类型名称，LN 插屏->1  map（LC01-> 1）
  override def makeTags(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]
    //广告位类型
    val adspacetype: Int = row.getAs[Int]("adspacetype")
//    广告位类型名称
    var adspacetypename=row.getAs[String]("adspacetypename")
    if(adspacetype>9)  map += "LC"+adspacetype -> 1
    else if(adspacetype<10) map += "LC0"+adspacetype -> 1
    if(StringUtils.isNotEmpty(adspacetypename))
      map+="LN"+adspacetypename->1
    map
  }
}

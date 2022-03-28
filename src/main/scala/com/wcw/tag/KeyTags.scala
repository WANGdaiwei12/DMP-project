package com.wcw.tag

import org.apache.spark.sql.Row

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 2:04
 * @version: V1.0
 * @modified By:
 */
object KeyTags extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    // 接收停用词变量
    val broadcastStopWord: Map[String, Int] = args(1).asInstanceOf[Map[String,Int]]
    //综艺娱乐|最新更新|综艺娱乐|国语|配音语种|情感|内地|新年特辑：脱单寻爱计划|类型|地区
    val kws: String = row.getAs[String]("keywords")
    kws.split("[|]").filter(str=>str.length>=3&&str.length<=8&&
    !broadcastStopWord.contains(str)).foreach(kw=>map += kw->1)
    map


  }
}

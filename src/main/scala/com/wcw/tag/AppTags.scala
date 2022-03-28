package com.wcw.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/28 下午 1:43
 * @version: V1.0
 * @modified By:
 */
object AppTags extends TagTrait {
  /*\
  2)（标签格式： APPxxxx->1）xxxx 为 App 名称，使用缓存文件   是不是要用redis  appname_dict 进行名称转换；APP 爱奇艺->1
3)渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
   */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    // 经过广播变量额判断。
    val broadcastAppNameMap = args(1).asInstanceOf[Map[String,String]]

    val appid: String = row.getAs[String]("appid")
    var appname: String = row.getAs[String]("appname")
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    if(StringUtils.isEmpty(appname)){
      val appname: String = broadcastAppNameMap.getOrElse(appid,"未知")
      //判断。
//      broadcastAppNameMap.contains(appid)
//      match{
//        case true => map+="APP"+appname ->1
//      }
     map += "APP"+appname -> 1
    }else{
      map += "APP"+appname -> 1
    }

    // 渠道标签
    map += "CN"+ adplatformproviderid -> 1

    map


  }
}

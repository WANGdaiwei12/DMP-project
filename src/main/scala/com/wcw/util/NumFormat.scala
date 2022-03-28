package com.wcw.util

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/24 下午 1:20
 * @version: V1.0
 * @modified By:
 */
object NumFormat {

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch{
      case _:Exception => 0
    }
  }

  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch{
      case _:Exception => 0
    }
  }


}

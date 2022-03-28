package com.wcw.util

import java.io.InputStream
import java.{lang, util}
import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/27 下午 10:41
 * @version: V1.0
 * @modified By:
 */
object JedisUtil {
def getJedis:Jedis={
  val loader: ClassLoader = JedisUtil.getClass.getClassLoader
  val inputStream: InputStream = loader.getResourceAsStream("redis.properties")
  val pros = new Properties
  var ip,passWord=""
  var port,timeout,database=0
  try {
    pros.load(inputStream)
   ip = pros.getProperty("redis.ip")
    port = pros.getProperty("redis.port").toInt
    timeout = pros.getProperty("redis.timeout").toInt
    passWord = pros.getProperty(" redis.passWord")
    database = pros.getProperty("redis.database").toInt
  }catch {
    case e:Exception=>{
     e.printStackTrace()
    }
  }
  val pool = new JedisPool(new GenericObjectPoolConfig(), ip, port, timeout, passWord, database)
  pool.getResource
}

  def main(args: Array[String]): Unit = {

    val jedis: Jedis = getJedis
//    val long: lang.Long = jedis.llen("county")
//    val str = jedis.lrange("county",0,long)
//val str: String = jedis.get("com.you.ku")

  }

//  def getJedis:Jedis={
//    val jedis: Jedis = getJedisPool.getResource
//    jedis
//  }
}

package com.wcw.business

import java.util
import java.util.Collections

import com.wcw.util.JedisUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/27 下午 11:38
 * @version: V1.0
 * @modified By:
 */
object AppintoRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("redis")
    val sparkContext: SparkContext = SparkContext.getOrCreate(conf)
    val Array(inputPath) = args
    val mappingAppname: RDD[String] = sparkContext.textFile(inputPath)

    mappingAppname.map(line=>{
      val word: Array[String] = line.split("[:]")
      (word(0),word(1))
    }).repartition(4).foreachPartition(ite=>{
      val jedis: Jedis = JedisUtil.getJedis
//      val map: Map[String, String] = ite.toMap
//      val map1 = new util.HashMap[String, String]()
//      map.foreach(x=> map1.put(x._1,x._2))
//      这是插入map类型
//      jedis.hmset("city10",map1)
            ite.foreach(e=>{
        jedis.set(e._1,e._2)
      })
      jedis.close()
    })

  }
}

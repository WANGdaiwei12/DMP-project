package com.wcw.business

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.wcw.util.TerritoryTool
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：地域报表 (RDD写法)
 * 省市/城市	原始请求	有效请求	广告请求	参与竞价数	竞价成功数	竞价成功率	展示量	点击量	点击率	广告成本	广告消费
 * -A 省		                          1000	600	30.56	800	800	2	15.87	15.87
 * B 市			                          200	100	30.56	178	178	2	15.87	15.87
 * C 市				                        100	50	30.56	78	78	2	15.87	15.87
 * D 市				                        400	200	30.56	324	324	2	15.87	15.87
 * F 市				                        300	250	30.56	167	167	2	15.87	15.87
 * +B 省				                      1000	600	23.23	700	323	4	98.21	50.81
 *
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/27 下午 6:17
 * @version: V1.0
 * @modified By:
 */
object TerritoryAna_RDD {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println(
        """
          |缺少参数
          |outpath mappinginpath inpath
          |""".stripMargin)
      sys.exit()
    }
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("diyu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkSession: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
import sparkSession.implicits._
    val Array(inpath, mappingpath,outpath) = args

    //    因为数据中有的数据有appid 但没有appname，此时需要使用mapping映射文件通过
//    appid获取对应的appname
    //    读取app_mapping.txt 文件。 .collec(). 使用广播变量进行广播。应该是个Map对象
    val mapping: RDD[String] = sc.textFile(mappingpath)
    val map: Map[String, String] = mapping.map(line => {
      val arr: Array[String] = line.split("[:]", -1)
      (arr(0), arr(1))
    }).collect().toMap
    val bro: Broadcast[Map[String, String]] = sc.broadcast(map)
    val df: DataFrame = sparkSession.read.parquet(inpath)

    // 获取每个列。
    val rdd: Dataset[(String, List[Double])] = df.map(row => {
      // 获取列。
      val requestMode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val device: String = row.getAs[String]("device")
      val networkmannername: String = row.getAs[String]("networkmannername")

      var appname: String = row.getAs[String]("appname")
      val appid: String = row.getAs[String]("appid")
      val qqs: List[Double] = TerritoryTool.qqsRtp(requestMode, processNode)
      val jingjia: List[Double] = TerritoryTool.jingjiaRtp(iseffective, isbilling, isbid, iswin, adorderid)
      val ggz: List[Double] = TerritoryTool.ggzjRtp(requestMode, iseffective)
      val mj: List[Double] = TerritoryTool.mjjRtp(requestMode, iseffective, isbilling)
      val ggc: List[Double] = TerritoryTool.ggcbRtp(iseffective, isbilling, iswin, winprice, adpayment)

      //      使用广播变量
      if (appname == "" || appname.isEmpty) {
        appname = bro.value.getOrElse(appid, "不明确")
      }
      (appname, qqs ++ jingjia ++ ggz ++ mj ++ ggc)
    })
    val res: RDD[(String, List[Double])] = rdd.rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    })
    res.saveAsTextFile(outpath)
  }


}

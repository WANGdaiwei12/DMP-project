package com.wcw.business

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 需求：地域报表 (dateframe写法)
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
object TerritoryAna {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println(
        """
          |缺少参数
          |outpath inpath
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

    val Array(inpath, outpath) = args
    val df: DataFrame = sparkSession.read.parquet(inpath)
    df.createTempView("log")
    var sql =
      """
        |with tmp as(
        |select
        |provincename,cityname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
        |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
        |from log
        |group by
        |provincename ,cityname
        |)
        |
        |select * from (
        |select tmp.provincename,"city",
        |sum(ysqq) ysqqsum,sum(yxqq),sum(ggqq),sum(xiaofei)
        |from  tmp
        |group by provincename
        |union
        |select provincename,cityname,ysqq,yxqq,ggqq,xiaofei from tmp) tmp1 order by provincename,ysqqsum desc
        |
      """.stripMargin
    val res: DataFrame = sparkSession.sql(sql)
    res.show(100)
//      写入mysql中
    val load: Config = ConfigFactory.load("sql.properties")
    val properties = new Properties()
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    res.write.jdbc("jdbc:mysql://localhost:3306/oneday?useSSL=false&serverTimezone=UTC",
      load.getString("jdbc.tableName"),properties)

  }


}

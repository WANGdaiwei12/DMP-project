package com.wcw.business

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.wcw.bean.Log
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Properties

/**
 * 统计各个省下面城市的排名，并写入mysql中
 * @description:${description}
 * @authror: snaker
 * @date: create:2022/3/24 下午 8:26
 * @version: V1.0
 * @modified By:
 */
object ProCityCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println(
        """
          |缺少参数
          |inpath
          |outpath
          |""".stripMargin)
      //      scala的类的一个方法
      sys.exit()
    }

    val Array(inpath, outpath) = args
    //
    val conf: SparkConf = new SparkConf()
      .setAppName("m1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context: SparkContext = SparkContext.getOrCreate(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._



    //    判断文件是否存在
    val configuration: Configuration = context.hadoopConfiguration
    val fileSystem: FileSystem = FileSystem.get(configuration)
    val path = new Path(outpath)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }

    val value: RDD[String] = context.textFile(inpath)
    value.map(_.split(",", -1))
      .filter(_.length >= 85)
      .map(x => Log(x))
      .toDS().write.format("parquet")
      .save(outpath)
    var otherPath=outpath
    val df: DataFrame = spark.read.format("parquet").load(outpath)
    df.createTempView("log")

    var sql =
      """
        |select provincename,cityname, pcsum,row_number()
        |over(partition by provincename sort by pcsum) as pnum from (
        |select provincename,cityname,count(*) as pcsum from log  group by provincename,cityname
        |) tmp
        |
        |""".stripMargin
//    val frame: DataFrame = spark.sql("select provincename,cityname,count(*) as pcsum from log  group by provincename,cityname ")

    val load: Config = ConfigFactory.load("sql.properties")
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    properties.setProperty("driver",load.getString("jdbc.driver"))

   spark.sql(sql)
     .write.mode(SaveMode.Append)
  .jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),
    properties)

  }


}

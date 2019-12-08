package mk.spark.washdata

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WashAnimeData {

  val  url="jdbc:mysql://192.168.31.233:3306/spider_ods?autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","lz")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")


  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setAppName("WashAnimeData")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    sparkConf.setMaster("local[*]")

    spark.read.jdbc(url,"bibi_season_info_ods",connectionProperties).show()



  }



}

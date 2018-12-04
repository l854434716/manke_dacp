package manke.dacp.spark

import java.util.function.Consumer
import java.util.{Map, Properties}

import manke.dacp.spark.ALSExample.{connectionProperties, url}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestMysqlRead {

  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")


  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setAppName("ALSExample")

    sparkConf.setMaster("local[*]")

    sparkConf.set("spark.sql.shuffle.partitions","3")

    val hadoopConf = new Configuration();
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("core-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("hdfs-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("yarn-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("hive-site.xml"))

    hadoopConf.iterator().forEachRemaining(new Consumer[java.util.Map.Entry[String,String]](){
      override def accept(t: Map.Entry[String, String]): Unit = {
        sparkConf.set(t.getKey,t.getValue)
      }
    })

    val spark = SparkSession
      .builder
      .config(sparkConf).enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    // $example on$
   // spark.read.jdbc(url,"t_bibi_long_comments",connectionProperties).createTempView("t_bibi_long_comments")
    spark.read.jdbc(url,"t_bibi_short_comments","mid",1000000,5000000,10,connectionProperties).createTempView("t_bibi_short_comments")

    println(spark.sql("select  *  from  t_bibi_short_comments").count())

    Thread.sleep(1000000)

    spark.stop()
  }

}

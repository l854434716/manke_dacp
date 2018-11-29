package manke.dacp.spark

import java.util.{Map, Properties}
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestMysqlWrite {

  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")


  def main(args: Array[String]): Unit = {
     val  sparkConf= new  SparkConf().setAppName("TestMysqlWrite").setMaster("local")

     val  sql= SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

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

    sql.sql("select  *  from manke_dw.t_bibi_anime_w2v_lsh_sim")
        .write.mode(SaveMode.Overwrite).jdbc(url,"t_bibi_anime_w2v_lsh_sim",connectionProperties)


     sql.stop()
  }

}

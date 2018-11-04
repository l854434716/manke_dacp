package manke.dacp.spark

import java.util.Map
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestHIveWrite {

  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setAppName("TestHIveWrite")

    sparkConf.setMaster("local[1]")

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



    val  sql=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    sql.sql("use manke_ods")

    sql.sql("insert  overwrite  table  a  select *  from  t_ods_anime_region")


    sql.stop()



  }

}

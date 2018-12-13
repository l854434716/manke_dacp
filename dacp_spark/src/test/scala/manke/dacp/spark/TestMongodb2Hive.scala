package manke.dacp.spark

import java.util.{Map => JMap}
import java.util.function.Consumer

import mk.spark.util.TimeUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object TestMongodb2Hive {

  def main(args: Array[String]): Unit = {

    var  _time =TimeUtil.getDayFirstTimeMills(System.currentTimeMillis(),-1)

    if (args.length==1){
      _time=args(0).toLong
    }

    val  sparkConf  = new  SparkConf().setAppName("TestMongodb2Hive")

    sparkConf.setMaster("local[*]")

    sparkConf.set("spark.sql.shuffle.partitions","3")

    val hadoopConf = new Configuration();
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("core-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("hdfs-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("yarn-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("hive-site.xml"))

    hadoopConf.iterator().forEachRemaining(new Consumer[JMap.Entry[String,String]](){
      override def accept(t: JMap.Entry[String, String]): Unit = {
        sparkConf.set(t.getKey,t.getValue)
      }
    })

    val  spark= SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val  longCommentsSummaryInputUri="mongodb://192.168.53.207:27017/spider.bibi_animes_comment_info"

    val originLongcommentsSummaryDf = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> longCommentsSummaryInputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()
    //新增的长评id
    import spark.implicits._
    val _ids=originLongcommentsSummaryDf
             .filter(_.getAs[Int]("ctime").toLong*1000>=_time)
             .select("_id").mapPartitions(_.map(_.getString(0).toLong)).collect().toIndexedSeq

    println(_ids)
    val  _longCommentsIdsBc= spark.sparkContext.broadcast(_ids)

    val  longCommentsInputUri="mongodb://192.168.53.207:27017/spider.bibi_animes_detail_comment"
    val originLongcommentsDf = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> longCommentsInputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()

    val lcds = originLongcommentsDf.filter(row=>{ _longCommentsIdsBc.value.contains(row.getAs[String]("_id").toLong)})
      .select("review_id", "media_id", "comment_detail").toDF("review_id", "media_id", "content")
      .show(5)


    val  shortCommentsInputUri="mongodb://192.168.53.207:27017/spider.bibi_animes_short_comment_info"
    val originShortCommentsDf = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> shortCommentsInputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()

    import  org.apache.spark.sql.functions._;

    val  getMediaId=udf((a1:String ,review_id:Int )=>{
      a1.substring(0,a1.length-review_id.toString.length+1).toInt
    })

    val  scds=originShortCommentsDf.filter(_.getAs[Int]("ctime").toLong*1000>=_time)//.filter(originShortCommentsDf("ctime")*1000 >=_time)
      .select($"review_id",getMediaId($"_id",$"review_id") as("media_id"),$"content",$"ctime")
      //.show(5)


  }

}

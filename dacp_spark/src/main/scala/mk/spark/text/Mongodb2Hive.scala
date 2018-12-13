package mk.spark.text

import mk.spark.util.TimeUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Mongodb2Hive {

  def main(args: Array[String]): Unit = {

    var  _time =TimeUtil.getDayFirstTimeMills(System.currentTimeMillis(),-1)

    if (args.length==1){
      _time=args(0).toLong
    }

    val  sparkConf  = new  SparkConf().setAppName("Mongodb2Hive")

    val  spark= SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val  longCommentsInputUri="mongodb://192.168.53.207:27017/spider.bibi_animes_detail_comment"
    val originLongcommentsDf = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> longCommentsInputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()

    val lcds = originLongcommentsDf.filter(originLongcommentsDf("ctime")*1000>=_time)
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
    import spark.implicits._
    val  scds=originShortCommentsDf.filter(originShortCommentsDf("ctime")*1000>_time)
      .select($"review_id",getMediaId($"_id",$"review_id") as("media_id"),$"content")
      .show(5)


  }

}
package mk.spark.text

import mk.spark.util.TImeUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Mongodb2Hive {

  def main(args: Array[String]): Unit = {

    var  _time =System.currentTimeMillis()

    if (args.length==1){
      _time=TImeUtil.getDayFirstTimeMills(args(0).toLong,-1)
    }

    val  sparkConf  = new  SparkConf().setAppName("Mongodb2Hive")

    val  spark= SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val  longCommentsInputUri="mongodb://192.168.53.207:27017/spider.bibi_animes_detail_comment"
    val longcommentsDf = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> longCommentsInputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()

    val originDf = longcommentsDf.filter(longcommentsDf("ctime") >=_time)
      .select("_id", "content", "imgTotalCount").toDF("id", "content", "imgnum")


    val  shortCommentsInputUri="mongodb://192.168.53.207:27017/spider.bibi_animes_short_comment_info"
    val shortCommentsDf = spark.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> shortCommentsInputUri,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"))
      .load()
  }

}

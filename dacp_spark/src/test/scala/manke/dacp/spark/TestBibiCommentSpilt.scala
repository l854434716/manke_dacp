package manke.dacp.spark

import java.util

import mk.spark.text.MediaWordNumber
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.Analysis
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.io.Source

object TestBibiCommentSpilt {

  def main(args: Array[String]): Unit = {

    val  sparkConf  = new  SparkConf().setAppName("BibiCommentSpilt").setMaster("local[*]")

    val  spark= SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val filter = new StopRecognition()
    //加入停用词
    filter.insertStopWords(util.Arrays.asList("r","n"))
    //加入停用词性
    filter.insertStopNatures("w",null,"ns","r","u","e")


    val  st=Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream("stopword.txt"))
      .getLines().toSeq



    filter.insertStopWords(st : _*)

    import   spark.implicits._
    spark.sql("select  media_id,comment_detail  from  manke_dw.bibi_long_comments  where  day='2018-12-17'")
      .mapPartitions(rows=>{
        val  analysis=getAnalysis()

        rows.flatMap(row=>{
          val  media_id=row.getString(0)
          val  comment_detail =row.getString(1)
          analysis.parseStr(comment_detail).recognition(filter).iterator().asScala
            .map(term=>MediaWordNumber(term.getRealName,media_id,1))

        })
      }).createOrReplaceTempView("bibi_long_comments_words")


    spark.sql("select  media_id,comment_detail  from  manke_dw.bibi_short_comments where  day='2018-12-17'")
      .mapPartitions(rows=>{
        val  analysis=getAnalysis()

        rows.flatMap(row=>{
          val  media_id=row.getInt(0).toString
          val  comment_detail =row.getString(1)
          analysis.parseStr(comment_detail).recognition(filter).iterator().asScala
            .map(term=>MediaWordNumber(term.getRealName,media_id,1))

        })
      }).createOrReplaceTempView("bibi_short_comments_words")


    spark.sql("select  word,media_id ,sum(num) as  word_num from bibi_short_comments_words  group by  media_id,word   sort by  word_num  desc")
        .show()



    spark.stop()


  }

  def  getAnalysis(): Analysis={

    new  ToAnalysis()
  }

}

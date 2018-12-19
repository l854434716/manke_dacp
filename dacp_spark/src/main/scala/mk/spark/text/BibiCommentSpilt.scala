package mk.spark.text

import java.util
import java.util.Properties

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.Analysis
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.io.Source

object BibiCommentSpilt {

  val filter = new StopRecognition()
  //加入停用词
  filter.insertStopWords(util.Arrays.asList("r","n"))
  //加入停用词性
  filter.insertStopNatures("w",null,"ns","r","u","e")


  val  st=Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream("stopword.txt"))
    .getLines().toSeq



  filter.insertStopWords(st : _*)


  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")


  def main(args: Array[String]): Unit = {

    val  sparkConf  = new  SparkConf().setAppName("BibiCommentSpilt")

    val  spark= SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    import   spark.implicits._
    spark.sql("select  media_id,comment_detail  from  manke_dw.bibi_long_comments")
      .mapPartitions(rows=>{
        val  analysis=getAnalysis()

        rows.flatMap(row=>{
          val  media_id=row.getString(0)
          val  comment_detail =row.getString(1)
          //analysis.parseStr(comment_detail).recognition(filter).iterator().asScala.filter(term=>{isNotBlankStr(term.getRealName)})
          spiltWord(comment_detail,analysis)
            .map(term=>MediaWordNumber(term.getRealName,media_id.toInt,1))

        })
      }).createOrReplaceTempView("bibi_long_comments_words")


    spark.sql("select  media_id,comment_detail  from  manke_dw.bibi_short_comments")
      .mapPartitions(rows=>{
        val  analysis=getAnalysis()

        rows.flatMap(row=>{
          val  media_id=row.getInt(0)
          val  comment_detail =row.getString(1)
          spiltWord(comment_detail,analysis)
            .map(term=>MediaWordNumber(term.getRealName,media_id,1))

        })
      }).createOrReplaceTempView("bibi_short_comments_words")



    spark.sql(" select  media_id,word,word_num  from ( select media_id,word,word_num, row_number() Over (partition by media_id  order by  word_num desc) line from (select  word,media_id ,sum(num) as  word_num from bibi_long_comments_words  group by  media_id,word ) t ) t1  where  line<11 ")
      .write.mode(SaveMode.Overwrite).jdbc(url,"bibi_long_comments_word_cloud",connectionProperties)

    spark.sql(" select  media_id,word,word_num  from ( select media_id,word,word_num, row_number() Over (partition by media_id  order by  word_num desc) line from (select  word,media_id ,sum(num) as  word_num from bibi_short_comments_words  group by  media_id,word ) t ) t1  where  line<11 ")
      .write.mode(SaveMode.Overwrite).jdbc(url,"bibi_short_comments_word_cloud",connectionProperties)




    spark.stop()


  }


  def  getAnalysis(): Analysis={

    new  ToAnalysis()
  }


  private   def    spiltWord(str:String, analysis: Analysis )={

    analysis.parseStr(str).recognition(filter).iterator().asScala.filter(term=>{isNotBlankStr(term.getRealName)})
    //.map(term=>MediaWordNumber(term.getRealName,media_id,1))
  }

  private  def  isNotBlankStr(str:String):Boolean={
    StringUtils.isNotBlank(str)&& StringUtils.length(str)>1 && !StringUtils.contains(str," ")//不包含特殊的字符
  }


}


case class  MediaWordNumber(word: String,media_id:Int,num:Int)

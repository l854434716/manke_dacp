package manke.dacp.spark.washdata

import java.util.Properties

import com.alibaba.fastjson.JSON
import mk.spark.washdata.Num2CHS
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.Iterator

object TestWashAnimeData {

  val  url="jdbc:mysql://192.168.31.233:3306/spider_ods?autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","lz")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")
  val  seasonReg = "第[\\u4e00-\\u9fa5]+季".r
  val  seasonNumReg = "第\\d+季".r
  val  numReg = "\\d+".r

  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setAppName("WashAnimeData")

    sparkConf.setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()


    spark.read.jdbc(url,"bibi_season_info_ods",connectionProperties).where("datediff(create_date,current_date())=0").createOrReplaceTempView("bibi")
    spark.read.jdbc(url,"qq_season_info_ods",connectionProperties).where("datediff(create_date,current_date())=0").createOrReplaceTempView("qq")
    spark.read.jdbc(url,"youku_season_info_ods",connectionProperties).where("datediff(create_date,current_date())=0").createOrReplaceTempView("youku")
    spark.read.jdbc(url,"douban_season_info_ods",connectionProperties).where("datediff(create_date,current_date())=0").createOrReplaceTempView("douban")

    //数据去重
    spark.udf.register("seasonNum2Chn",convertSeasonNameNum2CHN(_:String))
    spark
      .sql("select seasonNum2Chn(bibi.title) real_title,bibi.* from (select min(season_id) unique_season_id from  bibi group by title) t join bibi on  t.unique_season_id=bibi.season_id")
      .cache().createOrReplaceTempView("bibi")

    spark
      .sql("select seasonNum2Chn(qq.title) real_title,qq.* from (select max(id) u_id from  qq where region is not null and  views is  not null GROUP BY title) t join  qq on t.u_id=qq.id")
      .cache().createOrReplaceTempView("qq")
    spark
      .sql("select seasonNum2Chn(title) real_title,youku.*  from  youku where  title  is  not null")
      .cache().createOrReplaceTempView("youku")
    //获取豆瓣番剧第几季信息并拼凑至番剧名称中 优先取汉字如第二季
    def findSeasonName(name:String,alias:String) :String = {

      if(StringUtils.isEmpty(alias)){
        return name
      }
      val nameSeasonInfoList = seasonReg.findAllIn(name).toList
      val nameSeasonNumInfoList = seasonNumReg.findAllIn(name).toList
      if(nameSeasonInfoList.size>0||nameSeasonNumInfoList.size>0){
        return  name
      }
      val aliasSeasonInfoList = seasonReg.findAllIn(alias).toList
      val aliasSeasonNumInfoList = seasonNumReg.findAllIn(alias).toList

      if(aliasSeasonInfoList.size>0){
        return  StringUtils.join(name," ",aliasSeasonInfoList(0))
      }

      if(aliasSeasonNumInfoList.size>0){
        return  StringUtils.join(name," ",aliasSeasonNumInfoList(0))
      }
      name
    }

    spark.udf.register("findSeasonName",findSeasonName(_:String,_:String))
    spark.sql("select id,season_id,seasonNum2Chn(findSeasonName(name,alias)) real_title ,name,region,alias,duration,lang,rating_count,rating_value,date_published,types,create_date  from  douban where rating_value is not null")
      .createOrReplaceTempView("douban")

    spark
      .sql("select * from (select max(season_id)  un_season_id from  douban GROUP BY real_title) t  join   douban  on t.un_season_id=douban.season_id")
      .cache().createOrReplaceTempView("douban")
    //获取番剧名集合
    spark
      .sql("select real_title from  bibi union  select real_title from  qq union  select  real_title from  youku union  select  real_title from  douban").createTempView("titles")


    spark
      .sql("select titles.real_title as title ," +
        "douban.season_id douban_season_id,douban.region douban_region,duration,lang,rating_count,rating_value,date_published," +
        "media_id,is_started,is_finish,total_ep,status,danmakus,bibi.views bibi_views,favorites,bibi.score as b_score,count,can_watch,bibi.region bibi_region," +
        "qq.season_id qq_season_id,qq.region qq_region,pub_year,qq.score qq_score,mark_v,qq.views qq_views ,qq.update_info qq_update_info," +
        "youku.season_id youku_season,pub_web,youku.update_info youku_update_info,screen_time,youku.score youku_score" +
        " from  titles left join  bibi on  titles.real_title=bibi.real_title left join  qq on  titles.real_title=qq.real_title" +
        " left join youku on  titles.real_title = youku.real_title left join  douban on titles.real_title=douban.real_title")
      .write.mode(SaveMode.Overwrite).jdbc(url,"anime_season_info",connectionProperties)

    //获取番剧类型
    import  spark.implicits._
    spark.sql("select real_title title ,types from bibi  where types is not null ").as[TitleAndType]
      .mapPartitions(rows=>{
        rows.flatMap(row =>{
          val  types = row.types
          val  title = row.title
          import scala.collection.JavaConverters._
          JSON.parseArray(types,classOf[java.util.Map[String,String]])
            .asScala
            .map(t=>TitleAndType(title,t.get("name")))
        })
      }).createOrReplaceTempView("bibi_types")

    spark.sql("select real_title title,types  from   qq").as[TitleAndType]
      .mapPartitions(rows=>{
        rows.flatMap(row =>{
          val  types = row.types
          val  title = row.title
          import scala.collection.JavaConverters._
          JSON.parseObject(types,classOf[java.util.List[String]])
            .asScala
            .map(TitleAndType(title,_))
        })
      }).createOrReplaceTempView("qq_types")

    spark.sql("select real_title title ,types  from   youku  where types is not null ").as[TitleAndType]
      .mapPartitions(rows=>{
        rows.flatMap(row =>{
          val  types = row.types
          val  title = row.title
          StringUtils.split(types,"/").map(TitleAndType(title,_))
        })
      }).createOrReplaceTempView("youku_types")

    spark.sql("select real_title title ,types  from   douban").as[TitleAndType]
      .mapPartitions(rows=>{
        rows.flatMap(row =>{
          val  types = row.types
          val  title = row.title
          import scala.collection.JavaConverters._
          JSON.parseObject(types,classOf[java.util.List[String]])
            .asScala
            .map(TitleAndType(title,_))
        })
      }).createOrReplaceTempView("douban_types")

    spark.sql("select *  from  bibi_types union  select *  from  qq_types " +
      " union  select *  from  youku_types union select  *  from douban_types")
      .write.mode(SaveMode.Overwrite).jdbc(url,"anime_season_types",connectionProperties)


  }

  def convertSeasonNameNum2CHN(title:String):String ={

    val nameSeasonNumInfoList = seasonNumReg.findAllIn(title).toList

    if(nameSeasonNumInfoList.size==0) {
      return title
    }

    val  seasonName = nameSeasonNumInfoList(0)

    val  num = numReg.findAllIn(seasonName).toList(0).toInt


    StringUtils.replace(title,seasonName,StringUtils.join("第",Num2CHS.Convert(num),"季"))

  }

}

case class TitleAndType(title: String, types: String)

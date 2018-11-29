package mk.spark.recommend

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Word2Vec}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object BibiAnimeW2VEuclideanDistanceSim {
  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("BibiAnimeW2VEuclideanDistanceSim")

    val  sql=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sql.implicits._

    val word2Vec = new Word2Vec()
      .setInputCol("metadata")
      .setOutputCol("wordvec")
      .setVectorSize(15)
      .setMinCount(1);

    //这里将每一行的行号作为doc id，每一行的分词结果生成词频向量
    val sourcedf=sql.sql("select  id, split(metadata,',') as metadata from  manke_ods.t_ods_bibi_anime_metadata")


    val wvModel = word2Vec.fit(sourcedf);
    val w2vDf = wvModel.transform(sourcedf).drop("metadata");

    //获取LSH模型
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(4.0)
      .setNumHashTables(10)
      .setInputCol("wordvec")
      .setOutputCol("hashes")
    val brpModel = brp.fit(w2vDf)
    val tsDf = brpModel.transform(w2vDf)

    val brpDf = brpModel.approxSimilarityJoin(tsDf, tsDf, 0.005, "euclidean_distance")

    import org.apache.spark.sql.functions._

    val getIdFun=  udf((input:Row)=> {
      input.getInt(0)
    })


    val corrDf = brpDf.withColumn("season_id",getIdFun(col("datasetA")))
      .withColumn("compare_season_id",getIdFun(col("datasetB")))
      .drop("datasetA").drop("datasetB")
      .filter(row=>{row.getAs[Int]("season_id")!=row.getAs[Int]("compare_season_id")})
    .write.mode(SaveMode.Overwrite).jdbc(url,"t_bibi_anime_w2v_lsh_sim",connectionProperties)
      //.parquet("/user/hive/warehouse/manke_dw.db/t_bibi_anime_w2v_lsh_sim/");


  }

}

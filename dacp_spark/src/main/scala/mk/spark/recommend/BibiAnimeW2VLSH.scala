package mk.spark.recommend

import java.util.Map
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Word2Vec}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xiaojun on 2015/10/19.
  * https://blog.csdn.net/u013090676/article/details/82716911?utm_source=blogxgwz24
  */
object BibiAnimeW2VLSH {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("BibiAnimeW2VLSH")
    //sparkConf.set("spark.sql.shuffle.partitions","3")

    val  sql=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val word2Vec = new Word2Vec()
      .setInputCol("metadata")
      .setOutputCol("wordvec")
      .setVectorSize(15)
      .setMinCount(0);

    //这里将每一行的行号作为doc id，每一行的分词结果生成向量
    val sourcedf=sql.sql("select  id, split(metadata,',') as metadata from  manke_ods.t_ods_bibi_anime_metadata")


    val wvModel = word2Vec.fit(sourcedf);
    val w2vDf = wvModel.transform(sourcedf);

    //获取LSH模型
    val brp = new BucketedRandomProjectionLSH()
              .setBucketLength(4.0)
              .setNumHashTables(10)
              .setInputCol("wordvec")
              .setOutputCol("hashes")
    val brpModel = brp.fit(w2vDf)
    val tsDf = brpModel.transform(w2vDf)

    val brpDf = brpModel.approxSimilarityJoin(tsDf, tsDf, 0.015, "EuclideanDistance")

    import org.apache.spark.sql.functions._

     val getIdFun=  udf((input:Row)=> {
      input.getInt(0)
    })


    val corrDf = brpDf.withColumn("id",getIdFun(col("datasetA")))
      .withColumn("id_sim",getIdFun(col("datasetB")))
      .drop("datasetA").drop("datasetB").drop("EuclideanDistance");

    corrDf.createOrReplaceTempView("test");

    /*docSims.toDS().withColumnRenamed("_1","season_id")
            .withColumnRenamed("_2","compare_season_id")
            .withColumnRenamed("_3","cos_sim")
        /*.write.bucketBy(2,"season_id").mode(SaveMode.Overwrite)
        .saveAsTable("spark_create")*/
            .write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/manke_dw.db/t_bibi_anime_sim/")*/

    sql.stop()

  }
}


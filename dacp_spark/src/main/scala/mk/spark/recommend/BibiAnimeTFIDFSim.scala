package mk.spark.recommend

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.ml.linalg.{SparseVector => SV}
object BibiAnimeTFIDFSim {

  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BibiAnimeTFIDFSim")

    val  sql=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import sql.implicits._
    val hashingTF = new HashingTF().setInputCol("metadata").setOutputCol("tf_col")//.setNumFeatures(20)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量
    val sourcedf=sql.sql("select  id, split(metadata,',') as metadata from  manke_ods.t_ods_bibi_anime_metadata")


    val  tfDf=hashingTF.transform(sourcedf).drop("metadata")


    //构建idf model
    val idf = new IDF().setInputCol("tf_col").setOutputCol("idf_col")//.setMinDocFreq(1)
    //将tf向量转换成tf-idf向量
    val  iDFModel=idf.fit(tfDf)

    val tf_IdfDf=iDFModel.transform(tfDf).drop("tf_col")


    tf_IdfDf.createTempView("v_bibi_anime_tf_idf")

    sql.udf.register("cosSim",(sv1:SV,sv2:SV)=>{
      import breeze.linalg._
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
      bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
    })
    val  cosSimDf=
      sql.sql("select a.id as season_id  , b.id  as  compare_season_id,   cosSim(a.idf_col,b.idf_col) as  cos_sim  from  " +
        "v_bibi_anime_tf_idf a  cross join   v_bibi_anime_tf_idf b  where  a.id!=b.id")
    import   org.apache.spark.sql.functions._
    cosSimDf.filter(_.getAs[Double](2)>0).repartition(4,col("season_id"))
      .write.mode(SaveMode.Overwrite).jdbc(url,"t_bibi_anime_tfidf_cos_sim",connectionProperties)
    //.parquet("/user/hive/warehouse/manke_dw.db/t_bibi_anime_tfidf_cos_sim/")


  }

}

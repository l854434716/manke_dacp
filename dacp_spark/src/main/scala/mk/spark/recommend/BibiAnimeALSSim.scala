package mk.spark.recommend

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object BibiAnimeALSSim {

  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")

  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf().setAppName("BibiAnimeALSSim")

    val spark = SparkSession
      .builder
      .config(sparkConf).enableHiveSupport()
      .getOrCreate()

    spark.read.jdbc(url,"t_bibi_long_comments",connectionProperties).createTempView("t_bibi_long_comments")
    spark.read.jdbc(url,"t_bibi_short_comments","mid",1000000,5000000,10,connectionProperties).createTempView("t_bibi_short_comments")

    val  ratings=spark.sql("select media_id,mid,score  from  t_bibi_long_comments union  select media_id,mid,score  from  t_bibi_short_comments")

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("mid")
      .setItemCol("media_id")
      .setRatingCol("score")
    val model = als.fit(ratings.filter(_.get(1)!=null))

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test.filter(_.get(1)!=null))

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("score")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")


    spark.udf.register("cos_sim",(features1:Seq[Double],features2:Seq[Double])=>{
      import breeze.linalg._
      val bsv1 = new DenseVector[Double](features1.toArray)
      val bsv2 = new DenseVector[Double](features2.toArray)
      bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))

    })
    model.itemFactors.createTempView("item_factors")

    val  cosSimDf=
      spark.sql("select  a.id  as  media_id  , b.id  compare_media_id  , cos_sim(a.features,b.features) cos_sim  from  item_factors a  cross join  item_factors  b  where  a.id!=b.id ")

    cosSimDf.filter(_.getAs[Double](2)>0).repartition(5,col("media_id"))
      .write.mode(SaveMode.Overwrite)
      .jdbc(url,"t_bibi_anime_als_cos_sim",connectionProperties)
  }

}

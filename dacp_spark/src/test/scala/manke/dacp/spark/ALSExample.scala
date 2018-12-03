package manke.dacp.spark

import java.util.{Map, Properties}
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{SparseVector => SV}
//import  org.jblas.DoubleMatrix  breeze.linalg._   ml.linalg  区别

object ALSExample {

  val  url="jdbc:mysql://192.168.53.92:3306/mk?characterEncoding=utf-8&amp;autoReconnect=true";

  val  connectionProperties=new  Properties()
  connectionProperties.put("user","root")
  connectionProperties.put("password","cloudsmaker.net@123")
  connectionProperties.put("driver","com.mysql.jdbc.Driver")

  def main(args: Array[String]) {
    val  sparkConf = new SparkConf().setAppName("ALSExample")

    sparkConf.setMaster("local[*]")

    sparkConf.set("spark.sql.shuffle.partitions","3")

    val hadoopConf = new Configuration();
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("core-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("hdfs-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("yarn-site.xml"))
    hadoopConf.addResource(this.getClass.getClassLoader.getResourceAsStream("hive-site.xml"))

    hadoopConf.iterator().forEachRemaining(new Consumer[java.util.Map.Entry[String,String]](){
      override def accept(t: Map.Entry[String, String]): Unit = {
        sparkConf.set(t.getKey,t.getValue)
      }
    })

    val spark = SparkSession
      .builder
      .config(sparkConf).enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    // $example on$
    spark.read.jdbc(url,"t_bibi_long_comments",connectionProperties).createTempView("t_bibi_long_comments")
    //spark.read.jdbc(url,"t_bibi_short_comments","",1000,3000,10,connectionProperties).createTempView("t_bibi_short_comments")

    val  ratings=spark.sql("select media_id,mid,score  from  t_bibi_long_comments ")

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("mid")
      .setItemCol("media_id")
      .setRatingCol("score")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("score")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

   /* // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)*/
    // $example off$

    spark.udf.register("cos_sim",(sv1:SV,sv2:SV)=>{
      import breeze.linalg._
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
      bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))

    })

    model.itemFactors.createTempView("item_factors")
    val  cosSimDf=
    spark.sql("select  a.id  as  media_id  , b.id  compare_media_id  , cos_sim(a.features,b.features) cos_sim  from  item_factors a  cross join  item_factors  b  where  a.id!=b.id ")

    cosSimDf.filter(_.getAs[Double](2)>0).show()
    spark.stop()
  }
}
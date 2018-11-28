package manke.dacp.spark

import java.util.Map
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{SparseVector => SV}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by xiaojun on 2015/10/19.
  */
object TFIDFDemo1 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("TfIdfTest").setMaster("local")
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

    /*import  org.apache.spark.sql.functions._

    val  cosSim= udf((sv1:SV,sv2:SV)=>{
      import breeze.linalg._
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
      bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
    })

    val   cosSimDf=sql.sql("select a.id as id  , b.id  as  oid,  a.idf_col aidf , b.idf_col bidf   from  v_bibi_anime_tf_idf a  cross join   v_bibi_anime_tf_idf b  where  a.id!=b.id")
      .select($"id",$"oid", cosSim($"aidf",$"bidf"))*/

    sql.udf.register("cosSim",(sv1:SV,sv2:SV)=>{
      import breeze.linalg._
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
      bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
    })
    val  cosSimDf=
    sql.sql("select a.id as id  , b.id  as  oid,   cosSim(a.idf_col,b.idf_col) as  cos_sim_value  from  " +
      "v_bibi_anime_tf_idf a  cross join   v_bibi_anime_tf_idf b  where  a.id!=b.id")

    cosSimDf.show(100)
    /*val num_idf_pairs = tf_num_pairs.rdd.mapValues(v => idf.transform(v))
    //广播一份tf-idf向量集
    val b_num_idf_pairs = sql.sparkContext.broadcast(num_idf_pairs.collect())

    //计算doc之间余弦相似度
    val docSims = num_idf_pairs.flatMap {
      case (id1, idf1) =>
        val idfs = b_num_idf_pairs.value.filter(_._1 != id1)
        val sv1 = idf1.asInstanceOf[SV]
        import breeze.linalg._
        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
        idfs.map {
          case (id2, idf2) =>
            val sv2 = idf2.asInstanceOf[SV]
            val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
            val cosSim = bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
            (id1, id2, cosSim)
        }
    }.filter(_._3 >0)

    docSims.toDS().withColumnRenamed("_1","season_id")
            .withColumnRenamed("_2","compare_season_id")
            .withColumnRenamed("_3","cos_sim")
        /*.write.bucketBy(2,"season_id").mode(SaveMode.Overwrite)
        .saveAsTable("spark_create")*/
            .write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/manke_dw.db/t_bibi_anime_sim/")*/

    sql.stop()

  }
}


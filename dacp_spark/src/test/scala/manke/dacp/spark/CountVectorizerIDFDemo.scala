package manke.dacp.spark

import java.util.Map
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{SparseVector => SV}
object CountVectorizerIDFDemo {


  def main(args: Array[String]): Unit = {

      val  sparkConf=new  SparkConf().setMaster("local").setAppName("CountVectorizerIDFDemo")


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


    val  sourceDf=sql.sql("select  id, split(metadata,',') as metadata from  manke_ods.t_ods_bibi_anime_metadata")

    val cv = new CountVectorizer()
      .setInputCol("metadata")
      .setOutputCol("cv_col")
      //.setVocabSize(1000) //向量长度
      .setMinDF(1) //词汇出现次数必须大于等于1

    val  cvModel=cv.fit(sourceDf)

    val  cvDf=cvModel.transform(sourceDf).drop("metadata")

    //构建idf model
    val idf = new IDF().setInputCol("cv_col").setOutputCol("idf_col")//.setMinDocFreq(1)
    //将tf向量转换成idf向量
    val  iDFModel=idf.fit(cvDf)

    val  cv_idfDf=iDFModel.transform(cvDf).drop("cv_col")

    cv_idfDf.createTempView("v_bibi_anime_cv_idf")

    sql.udf.register("cosSim",(sv1:SV,sv2:SV)=>{
      import breeze.linalg._
      val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
      val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
      bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
    })


    val  cosSimDf=
      sql.sql("select a.id as id  , b.id  as  oid,   cosSim(a.idf_col,b.idf_col) as  cos_sim_value  from  " +
        "v_bibi_anime_cv_idf a  cross join   v_bibi_anime_cv_idf b  where  a.id!=b.id")


    cosSimDf.show(10)




  }

}

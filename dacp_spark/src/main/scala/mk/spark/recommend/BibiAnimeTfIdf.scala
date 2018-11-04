package mk.spark.recommend

import java.util.Map
import java.util.function.Consumer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.{SaveMode, SparkSession}

object BibiAnimeTfIdf {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("BibiAnimeTfIdf")

    val  sql=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import   sql.implicits._
    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量
    val tf_num_pairs=sql.sql("select  *  from  manke_ods.t_ods_bibi_anime_metadata")
      .mapPartitions(rows=>{
        rows.map(row=>{
          (row.getInt(0),hashingTF.transform(row.getString(1).split(',')))
        })
      })

    tf_num_pairs.cache()

    //构建idf model
    val idf = new IDF().fit(tf_num_pairs.rdd.values)
    //将tf向量转换成tf-idf向量
    val num_idf_pairs = tf_num_pairs.rdd.mapValues(v => idf.transform(v))
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
      .write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/manke_dw.db/t_bibi_anime_sim/")

    sql.stop()

  }

}

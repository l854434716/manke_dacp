package manke.dacp.spark

import java.util

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.Analysis
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

object TestWordCountTop20 {

  val stopWord=new StopRecognition()
  //加入停用词
  stopWord.insertStopWords(util.Arrays.asList("r","n"))
  //加入停用词性
  stopWord.insertStopNatures("w",null,"ns","r","u","e")


  val  st=Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream("stopword.txt"))
    .getLines().toSeq

  stopWord.insertStopWords(st : _*)

  def main(args: Array[String]): Unit = {
    val  sparkConf=new  SparkConf().setAppName("TestWordCountTop20")




    val    spark=SparkSession.builder().enableHiveSupport().config(sparkConf).enableHiveSupport().getOrCreate()
  }

  def   getAnalysis(): Analysis={
    new   ToAnalysis()
  }

}

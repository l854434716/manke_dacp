package manke.dacp.spark

import manke.dacp.spark.washdata.TestWashAnimeData
import org.apache.commons.lang3.StringUtils

object TestString {
  val  seasonReg = "第[\\u4e00-\\u9fa5]+季".r
  val  seasonNumReg = "第\\d+季".r


  def findSeasonName(name:String,alias:String) :String = {

    if(StringUtils.isEmpty(alias)){
      return name
    }
    val nameSeasonInfoList = seasonReg.findAllIn(name).toList
    val nameSeasonNumInfoList = seasonNumReg.findAllIn(name).toList
    if(nameSeasonInfoList.size>0||nameSeasonNumInfoList.size>0){
      return  name;
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

  def main(args: Array[String]): Unit = {

    //println(findSeasonName("银之守墓人","銀の墓守り 第2期/"))

    //println(TestWashAnimeData.convertSeasonNameNum2CHN("火影忍者 第季"))

    println(StringUtils.split("真人/格斗/科幻","/").toList)

  }


}
package mk.spark.util

object TImeUtil {

  val  MILLSINONEDAY= 1000* 60 * 60 * 24;

  val  EIGHTHOURSMILLS= 28800000l



  def   getDayFirstTimeMills(timeMills:Long,offset:Int):Long={

    timeMills-(timeMills+EIGHTHOURSMILLS)%MILLSINONEDAY+(offset*MILLSINONEDAY)

  }

}

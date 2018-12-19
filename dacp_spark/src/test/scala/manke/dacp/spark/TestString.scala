package manke.dacp.spark

import org.apache.commons.lang3.StringUtils

object TestString {

  def main(args: Array[String]): Unit = {

    println(" ".trim+"1")
    println(StringUtils.isNotBlank(" ".trim))

    println(" ".getBytes)
    println(" ".getBytes.toString)


  }


}
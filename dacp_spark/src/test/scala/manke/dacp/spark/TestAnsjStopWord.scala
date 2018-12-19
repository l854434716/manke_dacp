package manke.dacp.spark

import java.util

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.JavaConverters._
import scala.io.Source

object TestAnsjStopWord {

  def main(args: Array[String]): Unit = {


    val filter = new StopRecognition()
    //加入停用词
    filter.insertStopWords(util.Arrays.asList("r","n"))
    //加入停用词性
    filter.insertStopNatures("w",null,"ns","r","u","e")


   val  st=Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream("stopword.txt"))
                         .getLines().toSeq



    filter.insertStopWords(st : _*)
    val str="对于前期在回忆和虚幻的世界中渐进一些人生道理，这是比较少见的，比较耐人寻味思考。但东南西北代表的什么呢，标题的“兄弟”？像背心，裤子，虫和龙又代表什么呢？凭空出现让小叶突出勇敢？也没有具体的说明什么。也就围巾——戴假的名牌围巾被围堵这件事上说的通。\n后面心之所向回忆起来一切关于小光的记忆，回到了现实世界还能明白，逐渐把关于药厂的往事一个个摆上台，灵魂附体之类的还说的通。毕竟可以看到还是有点这样的设定在内，但后面灵魂出窍还带实体攻击，这在你所谓现实的世界上有些过于夸张的剧情。个人感觉最后一集很多地方说不通。表达的东西是表达了，但太过夸张反而让人有一丝不解，在一个现实世界出现太多所谓不是奇迹，而是不可思议缺又不能明白的现象去打击一个“官”之类的。\n脑回路太爆炸，却逻辑上还是有些问题所在。"

    System.out.println(ToAnalysis.parse(str).recognition(filter).toStringWithOutNature(","))
  }

}

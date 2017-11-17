import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.jsoup.Jsoup
import org.jsoup.nodes._
import java.util.Iterator
import scala.collection.JavaConverters._
import java.net.{ URL, MalformedURLException }
import scala.util.control.Exception._

object GetTextURL {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("GetTextURL").setMaster("local[1]")
      val sc = new SparkContext(conf)
      val html = scala.io.Source.fromURL("https://en.wikipedia.org/wiki/Donald_Trump").mkString
    
      def processNode(node: Node){       
        if(node.isInstanceOf[TextNode] && !node.toString().equals(" "))
          println(node.toString())
        if(node.childNodeSize()>0)
          node.childNodes().asScala.foreach(x => processNode(x))
      }
    
      val doc = Jsoup.parse(html)
      processNode(doc)
      //val list = html.split("\n").filter(_ != "")
      //val rdds = sc.parallelize(list)
      //val count = rdds.filter(_.contains("WWE")).count()
    //rdds.saveAsTextFile("/Users/vinaya/Downloads/text")
  }
} 
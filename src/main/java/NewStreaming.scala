import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._

object NewStreaming {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Wordcount")
    val ssc = new StreamingContext(conf, Seconds(1))
//    val lines = ssc.socketTextStream("localhost", 9092)
    val a = ssc.textFileStream("/home/akash/Downloads/test/s/a")
    a.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
